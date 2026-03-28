"""
test_underdog_lines.py
======================
Unit tests for UnderdogLinesFetcher.

Run with: pytest tests/test_underdog_lines.py -v
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from api.services.underdog_lines_fetcher import (
    STAT_MAP,
    UnderdogLinesFetcher,
)

# ---------------------------------------------------------------------------
# Fixture helpers — minimal Underdog API payload
# ---------------------------------------------------------------------------


def _make_player(player_id: str, first: str, last: str, pos: str, sport: str = "MLB") -> dict:
    return {
        "id": player_id,
        "first_name": first,
        "last_name": last,
        "position_name": pos,
        "position_display_name": pos,
        "sport_id": sport,
        "team_id": "team-001",
        "image_url": "",
        "dark_image_url": "",
        "light_image_url": "",
        "jersey_number": "99",
        "action_path": None,
        "country": None,
    }


def _make_game(game_id: int, title: str, sport: str = "MLB") -> dict:
    return {
        "id": game_id,
        "title": title,
        "abbreviated_title": title,
        "full_team_names_title": title,
        "short_title": title,
        "sport_id": sport,
        "scheduled_at": "2026-04-05T17:10:00Z",
        "status": "scheduled",
        "season_type": "regular",
        "year": 2026,
        "away_team_id": "away-001",
        "home_team_id": "home-001",
        "away_team_score": 0,
        "home_team_score": 0,
        "period": 0,
        "match_progress": "",
        "manually_created": False,
        "pre_game_data": {},
        "rank": 1,
        "rescheduled_from": None,
        "scoreboard_data": None,
        "title_suffix": None,
        "type": "Game",
        "updated_at": "2026-04-01T00:00:00Z",
    }


def _make_appearance(app_id: str, player_id: str, match_id: int) -> dict:
    return {
        "id": app_id,
        "player_id": player_id,
        "match_id": match_id,
        "match_type": "Game",
        "team_id": "team-001",
        "position_id": "pos-001",
        "badges": [],
        "lineup_status_id": None,
        "multiple_picks_allowed": True,
        "sort_by": 1,
        "type": "Player",
    }


def _make_option(choice: str, price: str, multiplier: str = "1.0") -> dict:
    short_label = f"H 5.5" if choice == "higher" else f"L 5.5"
    return {
        "id": f"opt-{choice}",
        "american_price": price,
        "choice": choice,
        "choice_display": choice.capitalize(),
        "choice_display_name_shorter": short_label,
        "choice_display_short": choice.capitalize(),
        "choice_id": f"{choice}__",
        "decimal_price": "1.9",
        "grouping_id": None,
        "odds": {"fantasy": {"multiplier": multiplier, "type": "modifier", "visual": {}}, "prediction": None, "sportsbook": None},
        "over_under_line_id": "line-001",
        "payout_multiplier": multiplier,
        "raw_probability": None,
        "selection_header": "Player",
        "selection_subheader": f"{choice} 5.5",
        "status": "active",
        "type": "OverUnderOption",
        "updated_at": "2026-04-01T00:00:00Z",
    }


def _make_line(
    line_id: str,
    app_id: str,
    stat: str,
    stat_value: str = "5.5",
    status: str = "active",
    line_type: str = "balanced",
    higher_price: str = "-115",
    lower_price: str = "-105",
) -> dict:
    return {
        "id": line_id,
        "stable_id": f"{line_id}|{line_type}",
        "contract_terms_url": None,
        "contract_url": None,
        "expires_at": None,
        "line_type": line_type,
        "live_event": False,
        "live_event_stat": None,
        "non_discounted_stat_value": None,
        "options": [
            _make_option("higher", higher_price),
            _make_option("lower", lower_price),
        ],
        "over_under": {
            "id": "ou-001",
            "appearance_stat": {
                "id": f"{app_id}-stat",
                "appearance_id": app_id,
                "display_stat": stat.replace("_", " ").title(),
                "graded_by": "count",
                "pickem_stat_id": "pstat-001",
                "rank": 1,
                "stat": stat,
            },
            "boost": None,
            "category": "player_prop",
            "display_mode": "default",
            "grid_display_title": stat,
            "has_alternates": False,
            "option_priority": "none",
            "prediction_market": False,
            "scoring_type_id": None,
            "team_divider": None,
            "title": f"Player {stat} O/U",
        },
        "over_under_id": "ou-001",
        "provider_id": "swish",
        "rank": 1000,
        "sort_by": -1000,
        "stat_value": stat_value,
        "status": status,
        "updated_at": "2026-04-01T00:00:00Z",
    }


def _build_payload(lines_config: list[dict]) -> dict:
    """Build a synthetic Underdog API response from config list."""
    players = []
    games = []
    appearances = []
    lines = []

    game = _make_game(101, "NYY @ SF")
    games.append(game)

    for i, cfg in enumerate(lines_config):
        p_id = f"player-{i:03d}"
        app_id = f"app-{i:03d}"
        players.append(
            _make_player(
                p_id,
                cfg.get("first", "Test"),
                cfg.get("last", f"Player{i}"),
                cfg.get("pos", "RF"),
                cfg.get("sport", "MLB"),
            )
        )
        appearances.append(_make_appearance(app_id, p_id, 101))
        lines.append(
            _make_line(
                f"line-{i:03d}",
                app_id,
                cfg["stat"],
                cfg.get("value", "5.5"),
                cfg.get("status", "active"),
                cfg.get("line_type", "balanced"),
                cfg.get("higher", "-115"),
                cfg.get("lower", "-105"),
            )
        )

    return {
        "over_under_lines": lines,
        "appearances": appearances,
        "players": players,
        "games": games,
        "solo_games": [],
        "opened_lines_count": len(lines),
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def fetcher():
    return UnderdogLinesFetcher()


@pytest.fixture
def mock_pitcher_payload():
    return _build_payload([
        {"stat": "strikeouts", "value": "5.5", "pos": "SP", "first": "Gerrit", "last": "Cole"},
        {"stat": "pitch_outs", "value": "15.5", "pos": "SP", "first": "Max",   "last": "Fried"},
        {"stat": "hits_allowed", "value": "4.5", "pos": "SP", "first": "Shane", "last": "Bieber"},
        {"stat": "walks_allowed", "value": "1.5", "pos": "RP", "first": "Pete",  "last": "Fairbanks"},
        {"stat": "runs_allowed", "value": "2.5", "pos": "SP", "first": "Corbin","last": "Burnes"},
    ])


@pytest.fixture
def mock_batter_payload():
    return _build_payload([
        {"stat": "hits",        "value": "1.5", "pos": "CF", "first": "Mike",  "last": "Trout"},
        {"stat": "total_bases", "value": "2.5", "pos": "SS", "first": "Trea",  "last": "Turner"},
        {"stat": "rbis",        "value": "0.5", "pos": "1B", "first": "Pete",  "last": "Alonso"},
        {"stat": "runs",        "value": "0.5", "pos": "LF", "first": "Juan",  "last": "Soto"},
        {"stat": "stolen_bases","value": "0.5", "pos": "2B", "first": "Jazz",  "last": "Chisholm"},
        {"stat": "home_runs",   "value": "0.5", "pos": "RF", "first": "Aaron", "last": "Judge"},
    ])


@pytest.fixture
def mock_mixed_payload():
    return _build_payload([
        {"stat": "strikeouts",  "value": "6.5", "pos": "SP", "first": "Logan", "last": "Webb"},
        {"stat": "hits",        "value": "1.5", "pos": "CF", "first": "Byron", "last": "Buxton"},
        {"stat": "unknown_stat","value": "1.5", "pos": "DH", "first": "No",    "last": "Mapping"},
        {"stat": "hits",        "value": "1.5", "pos": "LF", "first": "Buster","last": "Posey",
         "sport": "NBA"},  # non-MLB player — should be filtered
        {"stat": "pitch_outs",  "value": "18.5", "pos": "SP", "first": "Cy",   "last": "Young",
         "status": "suspended"},  # suspended — should be filtered when active_only=True
    ])


# ---------------------------------------------------------------------------
# Tests — _parse_american
# ---------------------------------------------------------------------------


class TestParseAmerican:
    def test_positive_odds(self, fetcher):
        assert fetcher._parse_american("+120") == 120

    def test_negative_odds(self, fetcher):
        assert fetcher._parse_american("-110") == -110

    def test_none_returns_default(self, fetcher):
        assert fetcher._parse_american(None) == -110

    def test_empty_string(self, fetcher):
        assert fetcher._parse_american("") == -110

    def test_float_string(self, fetcher):
        assert fetcher._parse_american("-115.0") == -115

    def test_garbage_returns_default(self, fetcher):
        assert fetcher._parse_american("N/A") == -110


# ---------------------------------------------------------------------------
# Tests — stat_map coverage
# ---------------------------------------------------------------------------


class TestStatMap:
    def test_pitcher_stats_present(self):
        pitcher_stats = [
            "strikeouts", "pitch_outs", "hits_allowed",
            "walks_allowed", "runs_allowed",
        ]
        for s in pitcher_stats:
            assert s in STAT_MAP, f"Missing pitcher stat: {s}"

    def test_batter_stats_present(self):
        batter_stats = [
            "hits", "total_bases", "rbis",
            "runs", "stolen_bases", "home_runs",
        ]
        for s in batter_stats:
            assert s in STAT_MAP, f"Missing batter stat: {s}"

    def test_propiq_prefixes(self):
        for ud_stat, propiq in STAT_MAP.items():
            assert propiq.startswith("pitcher_") or propiq.startswith("batter_"), (
                f"{ud_stat} → {propiq} has unexpected prefix"
            )


# ---------------------------------------------------------------------------
# Tests — pitcher line parsing
# ---------------------------------------------------------------------------


class TestPitcherLines:
    def test_parses_all_pitcher_stats(self, fetcher, mock_pitcher_payload):
        with patch.object(fetcher, "_fetch_with_retry", return_value=mock_pitcher_payload):
            lines = fetcher.fetch_mlb_lines()
        assert len(lines) == 5
        prop_types = {ln.prop_type for ln in lines}
        assert "pitcher_strikeouts" in prop_types
        assert "pitcher_outs" in prop_types
        assert "pitcher_hits_allowed" in prop_types
        assert "pitcher_walks_allowed" in prop_types
        assert "pitcher_earned_runs" in prop_types

    def test_pitcher_player_type(self, fetcher, mock_pitcher_payload):
        with patch.object(fetcher, "_fetch_with_retry", return_value=mock_pitcher_payload):
            lines = fetcher.fetch_mlb_lines()
        for ln in lines:
            assert ln.player_type == "pitcher"

    def test_strikeout_line_values(self, fetcher, mock_pitcher_payload):
        with patch.object(fetcher, "_fetch_with_retry", return_value=mock_pitcher_payload):
            lines = fetcher.fetch_mlb_lines()
        k_line = next(ln for ln in lines if ln.prop_type == "pitcher_strikeouts")
        assert k_line.line == 5.5
        assert k_line.player_name == "Gerrit Cole"

    def test_pitcher_filter(self, fetcher, mock_mixed_payload):
        with patch.object(fetcher, "_fetch_with_retry", return_value=mock_mixed_payload):
            lines = fetcher.fetch_mlb_lines()
        pitchers = fetcher.get_pitcher_lines(lines)
        for p in pitchers:
            assert p.player_type == "pitcher"


# ---------------------------------------------------------------------------
# Tests — batter line parsing
# ---------------------------------------------------------------------------


class TestBatterLines:
    def test_parses_all_batter_stats(self, fetcher, mock_batter_payload):
        with patch.object(fetcher, "_fetch_with_retry", return_value=mock_batter_payload):
            lines = fetcher.fetch_mlb_lines()
        assert len(lines) == 6
        prop_types = {ln.prop_type for ln in lines}
        assert "batter_hits" in prop_types
        assert "batter_total_bases" in prop_types
        assert "batter_rbis" in prop_types
        assert "batter_runs" in prop_types
        assert "batter_stolen_bases" in prop_types
        assert "batter_home_runs" in prop_types

    def test_batter_player_type(self, fetcher, mock_batter_payload):
        with patch.object(fetcher, "_fetch_with_retry", return_value=mock_batter_payload):
            lines = fetcher.fetch_mlb_lines()
        for ln in lines:
            assert ln.player_type == "batter"

    def test_batter_filter(self, fetcher, mock_mixed_payload):
        with patch.object(fetcher, "_fetch_with_retry", return_value=mock_mixed_payload):
            lines = fetcher.fetch_mlb_lines()
        batters = fetcher.get_batter_lines(lines)
        for b in batters:
            assert b.player_type == "batter"


# ---------------------------------------------------------------------------
# Tests — filtering
# ---------------------------------------------------------------------------


class TestFiltering:
    def test_active_only_filters_suspended(self, fetcher, mock_mixed_payload):
        with patch.object(fetcher, "_fetch_with_retry", return_value=mock_mixed_payload):
            lines = fetcher.fetch_mlb_lines(active_only=True)
        statuses = {ln.status for ln in lines}
        assert "suspended" not in statuses

    def test_active_only_false_includes_suspended(self, fetcher, mock_mixed_payload):
        with patch.object(fetcher, "_fetch_with_retry", return_value=mock_mixed_payload):
            lines = fetcher.fetch_mlb_lines(active_only=False)
        statuses = {ln.status for ln in lines}
        assert "suspended" in statuses

    def test_non_mlb_players_filtered(self, fetcher, mock_mixed_payload):
        with patch.object(fetcher, "_fetch_with_retry", return_value=mock_mixed_payload):
            lines = fetcher.fetch_mlb_lines()
        names = [ln.player_name for ln in lines]
        assert "Buster Posey" not in names  # NBA sport_id

    def test_unknown_stat_filtered(self, fetcher, mock_mixed_payload):
        with patch.object(fetcher, "_fetch_with_retry", return_value=mock_mixed_payload):
            lines = fetcher.fetch_mlb_lines()
        prop_types = {ln.prop_type for ln in lines}
        # "unknown_stat" has no mapping → should not appear
        assert all("unknown_stat" not in pt for pt in prop_types)

    def test_line_type_filter(self, fetcher):
        payload = _build_payload([
            {"stat": "strikeouts", "pos": "SP", "line_type": "balanced"},
            {"stat": "hits",       "pos": "RF", "line_type": "boosted"},
        ])
        with patch.object(fetcher, "_fetch_with_retry", return_value=payload):
            lines = fetcher.fetch_mlb_lines(line_types={"balanced"})
        assert all(ln.line_type == "balanced" for ln in lines)
        assert len(lines) == 1

    def test_get_lines_for_player(self, fetcher, mock_batter_payload):
        with patch.object(fetcher, "_fetch_with_retry", return_value=mock_batter_payload):
            lines = fetcher.fetch_mlb_lines()
        judge_lines = fetcher.get_lines_for_player(lines, "Aaron Judge")
        assert len(judge_lines) == 1
        assert judge_lines[0].prop_type == "batter_home_runs"

    def test_get_lines_by_prop_type(self, fetcher, mock_batter_payload):
        with patch.object(fetcher, "_fetch_with_retry", return_value=mock_batter_payload):
            lines = fetcher.fetch_mlb_lines()
        hit_lines = fetcher.get_lines_by_prop_type(lines, "batter_hits")
        assert all(ln.prop_type == "batter_hits" for ln in hit_lines)


# ---------------------------------------------------------------------------
# Tests — odds parsing
# ---------------------------------------------------------------------------


class TestOddsParsing:
    def test_american_odds_stored(self, fetcher, mock_pitcher_payload):
        with patch.object(fetcher, "_fetch_with_retry", return_value=mock_pitcher_payload):
            lines = fetcher.fetch_mlb_lines()
        for ln in lines:
            # Synthetic payload uses -115 / -105
            assert ln.higher_price == -115
            assert ln.lower_price == -105

    def test_game_title_present(self, fetcher, mock_pitcher_payload):
        with patch.object(fetcher, "_fetch_with_retry", return_value=mock_pitcher_payload):
            lines = fetcher.fetch_mlb_lines()
        for ln in lines:
            assert ln.game_title != ""

    def test_scheduled_at_present(self, fetcher, mock_pitcher_payload):
        with patch.object(fetcher, "_fetch_with_retry", return_value=mock_pitcher_payload):
            lines = fetcher.fetch_mlb_lines()
        for ln in lines:
            assert "2026" in ln.scheduled_at


# ---------------------------------------------------------------------------
# Tests — to_propiq_format
# ---------------------------------------------------------------------------


class TestPropIQFormat:
    def test_required_keys(self, fetcher, mock_pitcher_payload):
        with patch.object(fetcher, "_fetch_with_retry", return_value=mock_pitcher_payload):
            lines = fetcher.fetch_mlb_lines()
        propiq = fetcher.to_propiq_format(lines)
        required_keys = {
            "game_date", "player_name", "player_type", "prop_type",
            "sportsbook", "over_line", "under_line", "over_juice",
            "under_juice", "line_type", "fetched_at",
        }
        for record in propiq:
            assert required_keys.issubset(record.keys()), (
                f"Missing keys: {required_keys - record.keys()}"
            )

    def test_sportsbook_is_underdog(self, fetcher, mock_pitcher_payload):
        with patch.object(fetcher, "_fetch_with_retry", return_value=mock_pitcher_payload):
            lines = fetcher.fetch_mlb_lines()
        propiq = fetcher.to_propiq_format(lines)
        assert all(r["sportsbook"] == "underdog" for r in propiq)

    def test_game_date_format(self, fetcher, mock_pitcher_payload):
        with patch.object(fetcher, "_fetch_with_retry", return_value=mock_pitcher_payload):
            lines = fetcher.fetch_mlb_lines()
        propiq = fetcher.to_propiq_format(lines)
        for r in propiq:
            # Should be YYYY-MM-DD
            assert len(r["game_date"]) == 10
            assert r["game_date"][4] == "-"

    def test_over_under_line_equal(self, fetcher, mock_batter_payload):
        with patch.object(fetcher, "_fetch_with_retry", return_value=mock_batter_payload):
            lines = fetcher.fetch_mlb_lines()
        propiq = fetcher.to_propiq_format(lines)
        for r in propiq:
            assert r["over_line"] == r["under_line"]


# ---------------------------------------------------------------------------
# Tests — retry logic
# ---------------------------------------------------------------------------


class TestRetryLogic:
    def test_retries_on_connection_error(self, fetcher):
        import requests as req

        call_count = 0

        def flaky_get(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise req.exceptions.ConnectionError("timeout")
            mock_resp = MagicMock()
            mock_resp.raise_for_status.return_value = None
            mock_resp.json.return_value = {
                "over_under_lines": [], "appearances": [],
                "players": [], "games": [], "solo_games": [],
                "opened_lines_count": 0,
            }
            return mock_resp

        with patch.object(fetcher._session, "get", side_effect=flaky_get):
            with patch("api.services.underdog_lines_fetcher.time.sleep"):
                lines = fetcher.fetch_mlb_lines()
        assert call_count == 3
        assert lines == []

    def test_raises_after_max_retries(self, fetcher):
        import requests as req

        with patch.object(
            fetcher._session,
            "get",
            side_effect=req.exceptions.ConnectionError("always fails"),
        ):
            with patch("api.services.underdog_lines_fetcher.time.sleep"):
                with pytest.raises(req.exceptions.ConnectionError):
                    fetcher.fetch_mlb_lines()


# ---------------------------------------------------------------------------
# Tests — deduplication
# ---------------------------------------------------------------------------


class TestDeduplication:
    def test_no_duplicate_stable_ids(self, fetcher, mock_pitcher_payload):
        # Add a duplicate line (same stable_id)
        dupe = dict(mock_pitcher_payload["over_under_lines"][0])
        mock_pitcher_payload["over_under_lines"].append(dupe)

        with patch.object(fetcher, "_fetch_with_retry", return_value=mock_pitcher_payload):
            lines = fetcher.fetch_mlb_lines()

        stable_ids = [ln.stable_id for ln in lines]
        assert len(stable_ids) == len(set(stable_ids))
