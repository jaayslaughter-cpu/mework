"""
tests/test_odds_integration.py

Unit tests for:
    - SportsBooksReviewOddsFetcher (parse_response, parse_event, normalize_odds)
    - OddsFetcher.fetch_aggregated_odds (merge, CLV, arbitrage, dislocations)
    - MarketFusionEngine.run(), arbitrage_scan(), dislocation_scan()
    - EVHunter composite score (dislocation weight)
    - ArbitrageAgent filter_props (arb_margin gate, source filter)

All tests are pure-Python (no network, no ML deps).
"""

from __future__ import annotations

import sys
import os
import types
import unittest
from dataclasses import dataclass, field
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Bootstrap: make api.services importable without a full package install
# ---------------------------------------------------------------------------
REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# Stub out xmltodict so SBR XML tests work without the package
xmltodict_stub = types.ModuleType("xmltodict")
def _parse(xml_text):
    return {}
xmltodict_stub.parse = _parse
sys.modules.setdefault("xmltodict", xmltodict_stub)

# Stub pika + pika.exceptions so execution_agents imports cleanly
pika_exc = types.ModuleType("pika.exceptions")
pika_exc.AMQPConnectionError = type("AMQPConnectionError", (Exception,), {})
pika_exc.AMQPError           = type("AMQPError",           (Exception,), {})
sys.modules["pika.exceptions"] = pika_exc

pika_stub = types.ModuleType("pika")
pika_stub.URLParameters    = MagicMock()
pika_stub.BlockingConnection = MagicMock()
pika_stub.exceptions       = pika_exc
sys.modules["pika"] = pika_stub

# Stub requests at the top level so fetchers don't make real HTTP calls
import unittest.mock as mock_module
requests_mock = mock_module.MagicMock()
requests_mock.Session.return_value = mock_module.MagicMock()
sys.modules.setdefault("requests", requests_mock)

# Stub redis so execution_agents / apify_scrapers import cleanly
redis_stub = types.ModuleType("redis")
redis_stub.Redis = MagicMock()
redis_stub.StrictRedis = MagicMock()
redis_stub.exceptions = types.SimpleNamespace(RedisError=Exception, ConnectionError=Exception)
sys.modules.setdefault("redis", redis_stub)

# Stub apify_scrapers.DataEnricher so execution_agents imports cleanly
apify_stub = types.ModuleType("apify_scrapers")
apify_stub.DataEnricher = MagicMock
sys.modules.setdefault("apify_scrapers", apify_stub)


# ---------------------------------------------------------------------------
# Helpers — build minimal OddsLine / MergedOdds without full import
# ---------------------------------------------------------------------------
@dataclass
class _OddsLine:
    provider:      str
    player_name:   str
    prop_type:     str
    line:          float
    odds_over:     int
    odds_under:    int
    market_key:    str = ""
    game_id:       str = ""
    commence_time: str = ""
    timestamp:     float = 0.0


@dataclass
class _MergedOdds:
    player_name:           str
    prop_type:             str
    line:                  float
    consensus_prob_over:   float
    consensus_prob_under:  float
    best_odds_over:        int
    best_odds_under:       int
    best_over_provider:    str
    best_under_provider:   str
    clv_edge_pct:          float
    providers_sampled:     list = field(default_factory=list)
    game_id:               str = ""
    commence_time:         str = ""
    raw_lines:             list = field(default_factory=list)


# ---------------------------------------------------------------------------
# 1. SportsBooksReviewOddsFetcher — parse_event
# ---------------------------------------------------------------------------
class TestSBROddsFetcherParseEvent(unittest.TestCase):

    def setUp(self):
        # Import lazily inside test to use stubbed deps
        from api.services.sportsbookreview_odds_fetcher import (
            SportsBooksReviewOddsFetcher,
        )
        self.fetcher = SportsBooksReviewOddsFetcher.__new__(
            SportsBooksReviewOddsFetcher
        )

    def test_parse_event_basic(self):
        """parse_event extracts home/away, game_id, participants."""
        event = {
            "@homeTeam":  "Yankees",
            "@awayTeam":  "Red Sox",
            "@startTime": "2025-07-04T18:10:00Z",
            "@id":        "G123",
            "Participant": {
                "@name": "Gerrit Cole",
                "Odds": [
                    {"@type": "Over", "@price": "-115", "@line": "6.5", "@book": "Pinnacle"},
                    {"@type": "Under", "@price": "+100", "@line": "6.5", "@book": "Pinnacle"},
                ],
            },
        }
        result = self.fetcher.parse_event(event)
        self.assertIsNotNone(result)
        self.assertEqual(result["home_team"], "Yankees")
        self.assertEqual(result["game_id"], "G123")
        self.assertEqual(len(result["participants"]), 1)
        self.assertEqual(result["participants"][0]["player_name"], "Gerrit Cole")
        self.assertEqual(len(result["participants"][0]["odds"]), 2)

    def test_parse_event_multi_participant(self):
        """parse_event handles list of Participant nodes."""
        event = {
            "@homeTeam": "Cubs",
            "@awayTeam": "Cardinals",
            "@startTime": "2025-07-04T20:00:00Z",
            "@id": "G999",
            "Participant": [
                {
                    "@name": "Dylan Cease",
                    "Odds": {"@type": "Over", "@price": "-110", "@line": "7.5", "@book": "Circa"},
                },
                {
                    "@name": "Miles Mikolas",
                    "Odds": {"@type": "Under", "@price": "-105", "@line": "4.5", "@book": "Circa"},
                },
            ],
        }
        result = self.fetcher.parse_event(event)
        self.assertIsNotNone(result)
        self.assertEqual(len(result["participants"]), 2)

    def test_parse_event_missing_participants(self):
        """parse_event returns None when no valid participants found."""
        event = {"@homeTeam": "Dodgers", "@awayTeam": "Giants", "@id": "G0"}
        result = self.fetcher.parse_event(event)
        self.assertIsNone(result)

    def test_parse_event_malformed_price(self):
        """parse_event skips Odds nodes with non-numeric price."""
        event = {
            "@homeTeam": "Mets",
            "@awayTeam": "Phillies",
            "@startTime": "",
            "@id": "G777",
            "Participant": {
                "@name": "Max Scherzer",
                "Odds": {"@type": "Over", "@price": "N/A", "@line": "7.0", "@book": "Pinnacle"},
            },
        }
        result = self.fetcher.parse_event(event)
        # Participant with empty odds list → None
        self.assertIsNone(result)


# ---------------------------------------------------------------------------
# 2. SportsBooksReviewOddsFetcher — normalize_odds
# ---------------------------------------------------------------------------
class TestSBROddsFetcherNormalize(unittest.TestCase):

    def setUp(self):
        from api.services.sportsbookreview_odds_fetcher import (
            SportsBooksReviewOddsFetcher,
            OddsLine,
        )
        self.fetcher = SportsBooksReviewOddsFetcher.__new__(
            SportsBooksReviewOddsFetcher
        )
        self.OddsLine = OddsLine

    def _make_event(self, player: str, book: str, line: float,
                    over: int, under: int) -> dict:
        return {
            "home_team":   "Home",
            "away_team":   "Away",
            "start_time":  "2025-07-04T19:05:00Z",
            "game_id":     "G42",
            "prop_type":   "strikeouts",
            "market_key":  "pitcher-strikeouts",
            "participants": [{
                "player_name": player,
                "odds": [
                    {"type": "Over",  "price": over,  "line": line, "book": book},
                    {"type": "Under", "price": under, "line": line, "book": book},
                ],
            }],
        }

    def test_normalize_produces_odds_lines(self):
        """normalize_odds returns one OddsLine per (player, book, line)."""
        events = [self._make_event("Shane Bieber", "Pinnacle", 6.5, -115, +100)]
        lines = self.fetcher.normalize_odds(events)
        self.assertEqual(len(lines), 1)
        ol = lines[0]
        self.assertEqual(ol.player_name, "Shane Bieber")
        self.assertEqual(ol.prop_type, "strikeouts")
        self.assertEqual(ol.line, 6.5)
        self.assertEqual(ol.odds_over, -115)
        self.assertEqual(ol.odds_under, 100)
        self.assertIn("Pinnacle", ol.provider)

    def test_normalize_skips_zero_line(self):
        """normalize_odds drops entries where line == 0."""
        events = [self._make_event("Pitcher X", "SBR", 0.0, -110, -110)]
        lines = self.fetcher.normalize_odds(events)
        self.assertEqual(len(lines), 0)

    def test_normalize_multi_book(self):
        """normalize_odds creates separate OddsLine per book."""
        event = {
            "home_team":   "H",
            "away_team":   "A",
            "start_time":  "",
            "game_id":     "G1",
            "prop_type":   "strikeouts",
            "market_key":  "pitcher-strikeouts",
            "participants": [{
                "player_name": "Cole Ragans",
                "odds": [
                    {"type": "Over",  "price": -115, "line": 6.5, "book": "Pinnacle"},
                    {"type": "Under", "price": +100, "line": 6.5, "book": "Pinnacle"},
                    {"type": "Over",  "price": -110, "line": 6.5, "book": "BetOnline"},
                    {"type": "Under", "price": -110, "line": 6.5, "book": "BetOnline"},
                ],
            }],
        }
        lines = self.fetcher.normalize_odds([event])
        self.assertEqual(len(lines), 2)
        providers = {ol.provider for ol in lines}
        self.assertIn("SBR/Pinnacle", providers)
        self.assertIn("SBR/BetOnline", providers)


# ---------------------------------------------------------------------------
# 3. OddsFetcher — _strip_vig, merge_odds, fetch_aggregated_odds
# ---------------------------------------------------------------------------
class TestOddsFetcherMath(unittest.TestCase):

    def test_strip_vig_balanced(self):
        """_strip_vig removes symmetric vig (-110/-110 → 50/50)."""
        from api.services.odds_fetcher import _strip_vig
        p_over, p_under = _strip_vig(-110, -110)
        self.assertAlmostEqual(p_over, 0.5, places=4)
        self.assertAlmostEqual(p_under, 0.5, places=4)
        self.assertAlmostEqual(p_over + p_under, 1.0, places=6)

    def test_strip_vig_favourite(self):
        """Favourite side has higher true probability after vig strip."""
        from api.services.odds_fetcher import _strip_vig
        p_over, p_under = _strip_vig(-150, +130)
        self.assertGreater(p_over, p_under)
        self.assertAlmostEqual(p_over + p_under, 1.0, places=6)

    def test_american_to_implied_positive(self):
        """Positive American odds +200 → 33.3% implied."""
        from api.services.odds_fetcher import _american_to_implied
        self.assertAlmostEqual(_american_to_implied(200), 1/3, places=3)

    def test_american_to_implied_negative(self):
        """Negative American odds -110 → ~52.4% implied."""
        from api.services.odds_fetcher import _american_to_implied
        self.assertAlmostEqual(_american_to_implied(-110), 110/210, places=4)

    def test_prob_to_american_roundtrip(self):
        """Probability → American → implied should approximately roundtrip."""
        from api.services.odds_fetcher import _prob_to_american, _american_to_implied
        for prob in (0.55, 0.60, 0.45, 0.40):
            american = _prob_to_american(prob)
            implied  = _american_to_implied(american)
            self.assertAlmostEqual(implied, prob, delta=0.01)


class TestOddsFetcherMerge(unittest.TestCase):

    def _make_line(self, provider, player, prop, line, ov, un, game_id="G1"):
        return _OddsLine(
            provider=provider, player_name=player, prop_type=prop,
            line=line, odds_over=ov, odds_under=un, game_id=game_id,
        )

    def test_merge_groups_by_player_prop_line(self):
        """merge_odds groups same (player, prop, line) across providers."""
        from api.services.odds_fetcher import OddsFetcher, OddsLine
        fetcher = OddsFetcher.__new__(OddsFetcher)
        fetcher._providers = []

        # Two lines: same player/prop/line, different providers
        line1 = OddsLine("OddsAPI/dk",  "Aaron Judge", "home_runs", 0.5, -130, +110)
        line2 = OddsLine("SBR/Pinnacle","Aaron Judge", "home_runs", 0.5, -125, +105)
        merged = fetcher.merge_odds([line1, line2])

        self.assertEqual(len(merged), 1)
        m = merged[0]
        self.assertEqual(m.player_name.lower(), "aaron judge")
        self.assertEqual(len(m.providers_sampled), 2)

    def test_merge_clv_positive_when_sharp_has_heavy_under_vig(self):
        """
        CLV edge > 0 when:
          - Sharp (SBR/Pinnacle) has very heavy vig on the under side (-200)
            → its no-vig over prob is suppressed (~45.9%)
          - Soft book (OddsAPI/fanduel) offers a balanced -110/-110 line
            → no-vig over prob = 50.0%
          - best_over_line = soft (−110 > −130) → formula: 0.50/0.459 − 1 ≈ +0.09
        """
        from api.services.odds_fetcher import OddsFetcher, OddsLine
        fetcher = OddsFetcher.__new__(OddsFetcher)
        fetcher._providers = []

        # Sharp with heavy under vig suppresses its no-vig over probability
        sharp = OddsLine("SBR/Pinnacle",   "Freddie Freeman", "hits", 1.5, -130, -200)
        # Soft balanced book — best over odds, higher no-vig over prob
        soft  = OddsLine("OddsAPI/fanduel","Freddie Freeman", "hits", 1.5, -110, -110)
        merged = fetcher.merge_odds([sharp, soft])

        self.assertEqual(len(merged), 1)
        # soft no-vig over (0.50) > sharp no-vig over (0.459) → CLV > 0
        self.assertGreater(merged[0].clv_edge_pct, 0)

    def test_merge_clv_negative_when_soft_pays_less(self):
        """CLV edge is negative when soft book is worse than sharp on the over."""
        from api.services.odds_fetcher import OddsFetcher, OddsLine
        fetcher = OddsFetcher.__new__(OddsFetcher)
        fetcher._providers = []

        # Sharp prices Over as favourite -130; soft offers +105 (less probability → lower payout)
        sharp = OddsLine("SBR/Pinnacle",   "Freddie Freeman", "hits", 1.5, -130, +110)
        soft  = OddsLine("OddsAPI/fanduel","Freddie Freeman", "hits", 1.5, +105, -125)
        merged = fetcher.merge_odds([sharp, soft])
        self.assertEqual(len(merged), 1)
        # Soft no-vig over prob < sharp → clv_edge_pct < 0
        self.assertLess(merged[0].clv_edge_pct, 0)

    def test_merge_best_odds_selected(self):
        """merge_odds picks the highest over/under odds across providers."""
        from api.services.odds_fetcher import OddsFetcher, OddsLine
        fetcher = OddsFetcher.__new__(OddsFetcher)
        fetcher._providers = []

        lines = [
            OddsLine("OddsAPI/dk",  "Shohei Ohtani", "strikeouts", 8.5, -115, -105),
            OddsLine("OddsAPI/fd",  "Shohei Ohtani", "strikeouts", 8.5, -110, -110),
            OddsLine("SBR/Pinnacle","Shohei Ohtani", "strikeouts", 8.5, -120,  +95),
        ]
        merged = fetcher.merge_odds(lines)
        self.assertEqual(len(merged), 1)
        # Best over odds = -110 (fd), best under odds = +95 (Pinnacle)
        self.assertEqual(merged[0].best_odds_over, -110)
        self.assertEqual(merged[0].best_odds_under, 95)


class TestFetchAggregatedOdds(unittest.TestCase):

    def test_segments_present(self):
        """fetch_aggregated_odds returns top_clv, arbitrage, dislocations."""
        from api.services.odds_fetcher import OddsFetcher, MergedOdds, OddsLine

        fetcher = OddsFetcher.__new__(OddsFetcher)
        fetcher._providers = []

        # Inject a mock fetch_all and merge_odds
        m1 = MergedOdds(
            player_name="Bieber", prop_type="strikeouts", line=7.5,
            consensus_prob_over=0.60, consensus_prob_under=0.40,
            best_odds_over=-105, best_odds_under=+115,
            best_over_provider="OddsAPI/fd", best_under_provider="SBR/Pinnacle",
            clv_edge_pct=0.05, providers_sampled=["OddsAPI/fd", "SBR/Pinnacle"],
            raw_lines=[
                OddsLine("SBR/Pinnacle", "Bieber", "strikeouts", 7.5, -120, +100),
                OddsLine("OddsAPI/fd",   "Bieber", "strikeouts", 7.5, -105, +115),
            ],
        )
        # Arbitrage: total implied < 1.0
        m2 = MergedOdds(
            player_name="Judge", prop_type="home_runs", line=0.5,
            consensus_prob_over=0.50, consensus_prob_under=0.50,
            best_odds_over=+120, best_odds_under=+110,   # total implied < 1.0
            best_over_provider="OddsAPI/mg", best_under_provider="SBR/Circa",
            clv_edge_pct=0.08, providers_sampled=["OddsAPI/mg", "SBR/Circa"],
            raw_lines=[],
        )

        fetcher.fetch_all  = MagicMock(return_value=[])
        fetcher.merge_odds = MagicMock(return_value=[m1, m2])

        result = fetcher.fetch_aggregated_odds(n=10, min_clv_pct=0.02, min_dislocation_pct=0.03)
        self.assertIn("top_clv", result)
        self.assertIn("arbitrage", result)
        self.assertIn("dislocations", result)

    def test_arbitrage_segment_total_implied_below_one(self):
        """Only props with best_odds_over + best_odds_under < 1.0 implied go to arbitrage."""
        from api.services.odds_fetcher import OddsFetcher, MergedOdds, _american_to_implied

        fetcher = OddsFetcher.__new__(OddsFetcher)
        fetcher._providers = []

        # +120 over + +110 under: implied = 45.5% + 47.6% = 93.1% < 100% → arb!
        arb_candidate = MergedOdds(
            player_name="Judge", prop_type="home_runs", line=0.5,
            consensus_prob_over=0.5, consensus_prob_under=0.5,
            best_odds_over=120, best_odds_under=110,
            best_over_provider="A", best_under_provider="B",
            clv_edge_pct=0.08, providers_sampled=["A", "B"], raw_lines=[],
        )
        # -150 over + -130 under: total > 1.0 → NOT arb
        no_arb = MergedOdds(
            player_name="Freeman", prop_type="hits", line=1.5,
            consensus_prob_over=0.6, consensus_prob_under=0.4,
            best_odds_over=-150, best_odds_under=-130,
            best_over_provider="A", best_under_provider="B",
            clv_edge_pct=0.01, providers_sampled=["A", "B"], raw_lines=[],
        )

        fetcher.fetch_all  = MagicMock(return_value=[])
        fetcher.merge_odds = MagicMock(return_value=[arb_candidate, no_arb])

        result = fetcher.fetch_aggregated_odds(n=10)
        self.assertEqual(len(result["arbitrage"]), 1)
        self.assertEqual(result["arbitrage"][0].player_name, "Judge")


# ---------------------------------------------------------------------------
# 4. MarketFusionEngine
# ---------------------------------------------------------------------------
class TestMarketFusionEngine(unittest.TestCase):

    def _make_merged(self, player, clv=0.05, providers=2,
                     over=-110, under=-110) -> "_MergedOdds":
        return _MergedOdds(
            player_name=player, prop_type="strikeouts", line=7.5,
            consensus_prob_over=0.55, consensus_prob_under=0.45,
            best_odds_over=over, best_odds_under=under,
            best_over_provider="OddsAPI/fd", best_under_provider="SBR/Pinnacle",
            clv_edge_pct=clv,
            providers_sampled=[f"P{i}" for i in range(providers)],
            raw_lines=[],
        )

    def test_run_returns_list(self):
        """run() returns a list of PropEdge dicts."""
        from api.services.market_fusion import MarketFusionEngine
        engine = MarketFusionEngine()
        agg = {
            "top_clv":      [self._make_merged("Bieber", clv=0.06)],
            "arbitrage":    [],
            "dislocations": [],
        }
        engine._get_aggregated = MagicMock(return_value=agg)
        result = engine.run(n_top=10)
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["player_name"], "Bieber")

    def test_run_filters_low_clv(self):
        """run() drops props below clv_gate."""
        from api.services.market_fusion import MarketFusionEngine
        engine = MarketFusionEngine(clv_gate=0.05)
        agg = {
            "top_clv": [
                self._make_merged("Good",  clv=0.06),
                self._make_merged("Below", clv=0.03),   # below gate
            ],
            "arbitrage":    [],
            "dislocations": [],
        }
        engine._get_aggregated = MagicMock(return_value=agg)
        result = engine.run()
        names = [r["player_name"] for r in result]
        self.assertIn("Good", names)
        self.assertNotIn("Below", names)

    def test_arbitrage_scan_source_tag(self):
        """arbitrage_scan() stamps source='arbitrage'."""
        from api.services.market_fusion import MarketFusionEngine
        engine = MarketFusionEngine()
        # over=+120, under=+110 → total implied < 1.0
        arb_m = self._make_merged("Judge", clv=0.08, over=120, under=110)
        engine._get_aggregated = MagicMock(return_value={
            "top_clv": [], "arbitrage": [arb_m], "dislocations": [],
        })
        result = engine.arbitrage_scan()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["source"], "arbitrage")
        self.assertIn("arb_margin", result[0])
        self.assertGreater(result[0]["arb_margin"], 0)

    def test_dislocation_scan_source_tag(self):
        """dislocation_scan() stamps source='dislocation'."""
        from api.services.market_fusion import MarketFusionEngine, _compute_dislocation_score
        from api.services.odds_fetcher import OddsLine

        engine = MarketFusionEngine(dislocation_gate=0.03)

        # Build a MergedOdds with a sharp and soft line so _compute_dislocation_score fires
        from api.services.odds_fetcher import MergedOdds
        sharp_line = OddsLine("SBR/Pinnacle", "Cole", "strikeouts", 7.5, -130, +110)
        soft_line  = OddsLine("OddsAPI/fd",   "Cole", "strikeouts", 7.5, +105, -125)
        m = MergedOdds(
            player_name="Cole", prop_type="strikeouts", line=7.5,
            consensus_prob_over=0.57, consensus_prob_under=0.43,
            best_odds_over=105, best_odds_under=110,
            best_over_provider="OddsAPI/fd", best_under_provider="SBR/Pinnacle",
            clv_edge_pct=0.07, providers_sampled=["OddsAPI/fd", "SBR/Pinnacle"],
            raw_lines=[sharp_line, soft_line],
        )
        engine._get_aggregated = MagicMock(return_value={
            "top_clv": [], "arbitrage": [], "dislocations": [m],
        })
        result = engine.dislocation_scan(min_gap=0.0)   # no gap gate for this test
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["source"], "dislocation")
        self.assertGreater(result[0]["dislocation_score"], 0)


# ---------------------------------------------------------------------------
# 5. EVHunter composite score
# ---------------------------------------------------------------------------
class TestEVHunterCompositeScore(unittest.TestCase):

    def _make_prop(self, player, edge, dis_score=0.0, source="ml_engine"):
        class P:
            pass
        p = P()
        p.player_name = player
        p.edge_pct = edge
        p.source   = source
        p.dislocation_score = dis_score
        p.providers_sampled = ["P1", "P2"]
        return p

    def test_dislocation_bonus_promotes_clv_prop(self):
        """Prop with lower edge_pct but high dislocation_score outranks pure-edge prop."""
        # Import the real EVHunter
        import importlib.util, sys
        # We can't fully import execution_agents without pika, but pika is stubbed
        from execution_agents import EVHunter

        hunter = EVHunter.__new__(EVHunter)

        high_edge_no_dis  = self._make_prop("Alpha", edge=0.08, dis_score=0.00)
        lower_edge_high_dis = self._make_prop("Beta",  edge=0.06, dis_score=0.10)
        # composite for Beta = 0.06 + 0.10*0.5 = 0.11 > 0.08

        filtered = hunter.filter_props([high_edge_no_dis, lower_edge_high_dis])
        # Beta should rank first due to dislocation bonus
        self.assertEqual(filtered[0].player_name, "Beta")

    def test_positive_edge_gate(self):
        """EVHunter drops props with non-positive edge."""
        from execution_agents import EVHunter
        hunter = EVHunter.__new__(EVHunter)

        props = [
            self._make_prop("Good",  edge=0.05),
            self._make_prop("Zero",  edge=0.00),
            self._make_prop("Neg",   edge=-0.01),
        ]
        filtered = hunter.filter_props(props)
        names = [p.player_name for p in filtered]
        self.assertIn("Good", names)
        self.assertNotIn("Zero", names)
        self.assertNotIn("Neg", names)


# ---------------------------------------------------------------------------
# 6. ArbitrageAgent filter_props
# ---------------------------------------------------------------------------
class TestArbitrageAgentFilter(unittest.TestCase):

    def _make_prop(self, player, source, arb_margin, providers=2):
        class P:
            pass
        p = P()
        p.player_name = player
        p.edge_pct = 0.05
        p.source   = source
        p.arb_margin = arb_margin
        p.providers_sampled = [f"P{i}" for i in range(providers)]
        return p

    def test_filters_to_arbitrage_source_only(self):
        """ArbitrageAgent drops non-arbitrage source props."""
        from execution_agents import ArbitrageAgent
        agent = ArbitrageAgent.__new__(ArbitrageAgent)

        props = [
            self._make_prop("Arb1",   source="arbitrage",     arb_margin=0.01),
            self._make_prop("ML1",    source="ml_engine",     arb_margin=0.02),
            self._make_prop("Steam1", source="steam",         arb_margin=0.03),
        ]
        filtered = agent.filter_props(props)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0].player_name, "Arb1")

    def test_arb_gate_applied(self):
        """ArbitrageAgent drops arbitrage props below ARB_GATE."""
        from execution_agents import ArbitrageAgent
        agent = ArbitrageAgent.__new__(ArbitrageAgent)

        props = [
            self._make_prop("Below", source="arbitrage", arb_margin=0.002),  # < 0.005
            self._make_prop("Above", source="arbitrage", arb_margin=0.012),  # ≥ 0.005
        ]
        filtered = agent.filter_props(props)
        names = [p.player_name for p in filtered]
        self.assertIn("Above", names)
        self.assertNotIn("Below", names)

    def test_sorted_by_arb_margin_desc(self):
        """ArbitrageAgent returns props sorted by arb_margin descending."""
        from execution_agents import ArbitrageAgent
        agent = ArbitrageAgent.__new__(ArbitrageAgent)

        props = [
            self._make_prop("Small",  source="arbitrage", arb_margin=0.006),
            self._make_prop("Large",  source="arbitrage", arb_margin=0.025),
            self._make_prop("Medium", source="arbitrage", arb_margin=0.014),
        ]
        filtered = agent.filter_props(props)
        margins = [p.arb_margin for p in filtered]
        self.assertEqual(margins, sorted(margins, reverse=True))

    def test_requires_min_two_providers(self):
        """ArbitrageAgent drops single-provider arbitrage (data artefact guard)."""
        from execution_agents import ArbitrageAgent
        agent = ArbitrageAgent.__new__(ArbitrageAgent)

        props = [
            self._make_prop("One",  source="arbitrage", arb_margin=0.01, providers=1),
            self._make_prop("Two",  source="arbitrage", arb_margin=0.01, providers=2),
        ]
        filtered = agent.filter_props(props)
        names = [p.player_name for p in filtered]
        self.assertIn("Two", names)
        self.assertNotIn("One", names)


if __name__ == "__main__":
    unittest.main(verbosity=2)
