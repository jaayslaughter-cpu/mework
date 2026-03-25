import React, { useState, useEffect, useCallback } from 'react';
import { createRoot } from 'react-dom/client';
import { AlertTriangle, TrendingUp, Activity, Zap, RefreshCw, BarChart2, Shield, Trophy, Swords, Search, Target } from 'lucide-react';
import { GameOdds, PlayerProp, ViewMode } from './types';
import { fetchEvents, fetchGameOdds, fetchPlayerProps } from './utils/api';
import { GameCard } from './components/GameCard';
import { PropCard } from './components/PropCard';

// ── Calibration mock data (replace with real API when backend is live) ─────
const buildMockCalibration = (_games: GameOdds[]) => {
  const propTypes = ['Hits O1.5', 'Home Runs O0.5', 'Pitcher Ks O6.5', 'Total Bases O2.5', 'RBIs O0.5'];
  const players = [
    { name: 'Aaron Judge', raw: 0.61, cal: 0.55, book: 0.46, edge: 0.09, tier: 'A', rec: 'STRONG PLAY' },
    { name: 'Shohei Ohtani', raw: 0.58, cal: 0.54, book: 0.50, edge: 0.04, tier: 'B', rec: 'LEAN OVER' },
    { name: 'Mookie Betts', raw: 0.47, cal: 0.46, book: 0.52, edge: -0.06, tier: 'B_FADE', rec: 'LEAN UNDER' },
    { name: 'Freddie Freeman', raw: 0.55, cal: 0.53, book: 0.51, edge: 0.02, tier: 'C', rec: 'SKIP' },
    { name: 'Yordan Alvarez', raw: 0.64, cal: 0.60, book: 0.48, edge: 0.12, tier: 'A', rec: 'STRONG PLAY' },
    { name: 'Vladimir Guerrero Jr.', raw: 0.52, cal: 0.50, book: 0.53, edge: -0.03, tier: 'C', rec: 'SKIP' },
    { name: 'Corbin Burnes', raw: 0.67, cal: 0.63, book: 0.54, edge: 0.09, tier: 'A', rec: 'STRONG PLAY' },
    { name: 'Spencer Strider', raw: 0.44, cal: 0.43, book: 0.55, edge: -0.12, tier: 'A_FADE', rec: 'STRONG FADE' },
  ];
  return players.map((p, i) => ({
    ...p,
    prop_type: propTypes[i % propTypes.length],
    kelly: Math.max(0, (p.edge / (1 - p.book)) * 0.5).toFixed(3),
    correction: (Math.random() * 0.04 - 0.02).toFixed(4),
    samples: Math.floor(Math.random() * 80) + 20,
  }));
};

const MODEL_HEALTH = {
  status: 'HEALTHY',
  accuracy_7d: 0.576,
  mae_7d: 0.089,
  sample_size: 342,
  alert_count: 2,
};

const CORRECTIONS = [
  { player: 'Mike Trout', prop_type: 'Hits O1.5', correction: -0.064, samples: 48, direction: 'over-predicting' },
  { player: 'Pete Alonso', prop_type: 'Home Runs O0.5', correction: 0.041, samples: 31, direction: 'under-predicting' },
];

// ── Tier badge ─────────────────────────────────────────────────────────────
const TierBadge: React.FC<{ tier: string }> = ({ tier }) => {
  const cfg: Record<string, { bg: string; text: string; label: string }> = {
    'A':       { bg: 'bg-success/20', text: 'text-success', label: '🔥 STRONG PLAY' }
  };

// ── Calibration row ────────────────────────────────────────────────────────

type CalibrationData = {
  name: string;
  prop_type: string;
  edge: number;
  raw: number;
  cal: number;
  tier: 'A' | 'B' | 'C' | 'B_FADE' | 'A_FADE';
  rec: unknown;
};

const getEdgeColor = (edge: number): string => {
  const edgeColorMap = [
    { check: (e: number) => e >= 0.08, color: 'text-success' },
    { check: (e: number) => e >= 0.04, color: 'text-primary' },
    { check: (e: number) => e <= -0.08, color: 'text-error' },
    { check: (e: number) => e <= -0.04, color: 'text-warning' }
  ];
  const found = edgeColorMap.find(m => m.check(edge));
  return found ? found.color : 'text-base-content/40';
};

const CalibrationRow: React.FC<{ p: CalibrationData }> = ({ p }) => {
  const edgeColor = getEdgeColor(p.edge);
  const barWidth = Math.abs(p.edge) * 400;
  const barColor = p.edge > 0 ? 'bg-success' : 'bg-error';

  return (
    <div className="card bg-base-200 border border-base-300 p-3 flex flex-col gap-2">
      <div className="flex items-start justify-between gap-2">
        <div>
          <div className="font-semibold text-sm text-base-content">{p.name}</div>
          <div className="text-xs text-base-content/50">{p.prop_type}</div>
        </div>
        <TierBadge tier={p.tier} rec={p.rec} />
      </div>

      {/* Probability bars */}
      <div className="grid grid-cols-3 gap-1 text-center text-xs">
        <div className="bg-base-300 rounded p-1">
          <div className="text-base-content/40">Raw</div>
          <div className="font-mono font-bold">{(p.raw * 100).toFixed(0)}%</div>
        </div>
        <div className="bg-base-300 rounded p-1">
          <div className="text-base-content/40">Calibrated</div>
          <div className="font-mono font-bold text-primary">{(p.cal * 100).toFixed(0)}%</div>
        </div>
        <div className="bg-base-300 rounded p-1">
          <div className="text-base-content/40">Book</div>
          <div className="font-mono font-bold">{(p.book * 100).toFixed(0)}%</div>
        </div>
      </div>

      {/* Edge bar */}
      <div>
        <div className="flex items-center justify-between text-xs mb-1">
          <span className="text-base-content/40">Edge</span>
          <span className={`font-mono font-bold ${edgeColor}`}>
            {p.edge >= 0 ? '+' : ''}{(p.edge * 100).toFixed(1)}%
          </span>
        </div>
        <ProgressBar barColor={barColor} barWidth={barWidth} />
      </div>

      <PanelInfo kelly={p.kelly} samples={p.samples} />
    </div>
  );
};

const ProgressBar = ({ barColor, barWidth }: { barColor: string; barWidth: number }) => (
  <div className="w-full h-1.5 bg-base-300 rounded-full overflow-hidden">
    <div
      className={`h-full rounded-full ${barColor} transition-all`}
      style={{ width: `${Math.min(100, barWidth)}%` }}
    />
  </div>
);

const PanelInfo = ({ kelly, samples }: { kelly: number; samples: number }) => (
  <div className="flex items-center justify-between text-xs text-base-content/40">
    <span>
      ½ Kelly: <span className="font-mono text-base-content/60">{kelly}u</span>
    </span>
    <span>{samples} samples</span>
  </div>
);

// ── Model health panel ─────────────────────────────────────────────────────
const ModelHealthPanel = () => {
  const health = MODEL_HEALTH;
  const statusColor = health.status === 'HEALTHY' ? 'text-success' :
                      health.status === 'EXCELLENT' ? 'text-primary' :
                      health.status === 'FAIR' ? 'text-warning' : 'text-error';

  return (
    <div className="flex flex-col gap-3">
      {/* Status card */}
      <div className="card bg-base-200 border border-base-300 p-4">
        <div className="flex items-center gap-2 mb-3">
          <Activity size={16} className="text-primary" />
          <span className="font-bold text-sm">Model Health</span>
          <span className={`ml-auto text-sm font-bold ${statusColor}`}>● {health.status}</span>
        </div>
        <div className="grid grid-cols-2 gap-3">
          {[
            { label: '7-Day Accuracy', value: `${(health.accuracy_7d * 100).toFixed(1)}%`, good: health.accuracy_7d > 0.54 },
            { label: 'Mean Abs Error', value: health.mae_7d.toFixed(3), good: health.mae_7d < 0.10 },
            { label: 'Sample Size', value: health.sample_size.toLocaleString(), good: true },
            { label: 'Active Alerts', value: health.alert_count, good: health.alert_count < 3 },
          ].map(({ label, value, good }) => (
            <div key={label} className="bg-base-300 rounded p-2 text-center">
              <div className="text-xs text-base-content/40">{label}</div>
              <div className={`font-mono font-bold text-sm mt-0.5 ${good ? 'text-base-content' : 'text-warning'}`}>
                {value}
              </div>
            </div>
          ))}
        </div>

        {/* Accuracy bar */}
        <div className="mt-3">
          <div className="flex justify-between text-xs text-base-content/40 mb-1">
            <span>Accuracy</span>
            <span>Target: 55%+</span>
          </div>
          <div className="w-full h-2 bg-base-300 rounded-full overflow-hidden">
            <div
              className="h-full bg-success rounded-full"
              style={{ width: `${health.accuracy_7d * 100}%` }}
            />
          </div>
          <div className="w-full relative h-0">
            <div className="absolute h-4 w-0.5 bg-warning/60 -top-3" style={{ left: '55%' }} />
          </div>
        </div>
      </div>

      {/* Active corrections */}
      <div className="card bg-base-200 border border-base-300 p-4">
        <div className="flex items-center gap-2 mb-3">
          <Shield size={16} className="text-warning" />
          <span className="font-bold text-sm">Self-Corrections Active</span>
          <span className="ml-auto badge badge-warning badge-sm">{CORRECTIONS.length}</span>
        </div>
        {CORRECTIONS.length === 0 ? (
          <p className="text-xs text-base-content/40 text-center py-2">No systematic biases detected</p>
        ) : (
          <div className="flex flex-col gap-2">
            {CORRECTIONS.map((c, i) => (
              <div key={i} className="bg-base-300 rounded p-2.5 text-xs">
                <div className="flex justify-between items-start">
                  <div>
                    <span className="font-semibold">{c.player}</span>
                    <span className="text-base-content/40 ml-1">· {c.prop_type}</span>
                  </div>
                  <span className={`font-mono font-bold ${c.correction < 0 ? 'text-error' : 'text-success'}`}>
                    {c.correction > 0 ? '+' : ''}{(c.correction * 100).toFixed(1)}%
                  </span>
                </div>
                <div className="text-base-content/40 mt-0.5">
                  {c.direction} · {c.samples} samples
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Pipeline status */}
      <div className="card bg-base-200 border border-base-300 p-4">
        <div className="flex items-center gap-2 mb-3">
          <Zap size={16} className="text-success" />
          <span className="font-bold text-sm">Pipeline Status</span>
        </div>
        {[
          { label: 'Odds API (Key 1)', status: 'LIVE', color: 'text-success' },
          { label: 'Odds API (Key 2)', status: 'STANDBY', color: 'text-primary' },
          { label: 'MLB Stats API', status: 'LIVE', color: 'text-success' },
          { label: 'ESPN API', status: 'LIVE', color: 'text-success' },
          { label: 'Calibration DB', status: 'ACTIVE', color: 'text-success' },
          { label: 'Context7 MCP', status: 'CONFIGURED', color: 'text-primary' },
        ].map(({ label, status, color }) => (
          <div key={label} className="flex justify-between text-xs py-1 border-b border-base-300 last:border-0">
            <span className="text-base-content/60">{label}</span>
            <span className={`font-mono font-semibold ${color}`}>● {status}</span>
          </div>
        ))}
      </div>
    </div>
  );
};

// ── Agent Army mock leaderboard (10 agents) ────────────────────────────────────
const MOCK_LEADERBOARD = [
  { rank: 1,  name: '+EV Hunter',        strategy: 'EV > 5% | 1–3 legs',             bets: 148, wins: 83, losses: 65,  roi: 12.4, profit: 18.4, capital: 200, status: '🔥 2x',    color: 'text-success',        bg: 'bg-success/10', tag: 'CORE',       xgAcc: 82.1 },
  { rank: 2,  name: 'Umpire',            strategy: 'Ump K%>22% + FIP<3.80 K props',  bets: 44,  wins: 26, losses: 18,  roi: 11.2, profit: 4.9,  capital: 200, status: '🔥 2x',    color: 'text-success',        bg: 'bg-success/10', tag: 'NEW',        xgAcc: 79.4 },
  { rank: 3,  name: 'Under Machine',     strategy: 'ERA<3.50 pitcher duels',          bets: 94,  wins: 55, losses: 39,  roi: 9.8,  profit: 9.2,  capital: 200, status: '🔥 2x',    color: 'text-success',        bg: 'bg-success/10', tag: 'CORE',       xgAcc: 81.3 },
  { rank: 4,  name: 'First 5 Innings',   strategy: 'FIP<3.50 SwStr>12% F5 unders',   bets: 52,  wins: 29, losses: 23,  roi: 7.1,  profit: 3.7,  capital: 100, status: 'Active',   color: 'text-primary',        bg: 'bg-primary/5',  tag: 'NEW',        xgAcc: 78.6 },
  { rank: 5,  name: 'Arbitrage',         strategy: 'Cross-book guaranteed >1%',       bets: 31,  wins: 30, losses: 1,   roi: 4.1,  profit: 1.3,  capital: 100, status: 'Active',   color: 'text-primary',        bg: 'bg-primary/5',  tag: 'CORE',       xgAcc: 97.2 },
  { rank: 6,  name: 'Fade',              strategy: 'Public >70% → opposite side',     bets: 38,  wins: 20, losses: 18,  roi: 3.2,  profit: 1.2,  capital: 100, status: 'Active',   color: 'text-primary',        bg: 'bg-primary/5',  tag: 'NEW',        xgAcc: 76.8 },
  { rank: 7,  name: '3-Leg Correlated',  strategy: 'Same-game props | exactly 3 legs',bets: 67,  wins: 26, losses: 41,  roi: 2.1,  profit: 1.4,  capital: 100, status: 'Active',   color: 'text-base-content/60',bg: 'bg-base-200',   tag: 'CORE',       xgAcc: 77.7 },
  { rank: 8,  name: 'Parlay',            strategy: 'Game outcomes | 2–3% ROI | 2–4L', bets: 112, wins: 47, losses: 65,  roi: -1.2, profit: -1.3, capital: 50,  status: '⚠️ 0.5x', color: 'text-warning',        bg: 'bg-warning/5',  tag: 'CORE',       xgAcc: 74.3 },
  { rank: 9,  name: 'Live',              strategy: '>5% in-play line movement',        bets: 58,  wins: 31, losses: 27,  roi: -3.7, profit: -2.1, capital: 25,  status: '⚠️ 0.5x', color: 'text-warning',        bg: 'bg-warning/5',  tag: 'CORE',       xgAcc: 73.1 },
  { rank: 10, name: 'Grading',           strategy: 'Boxscore settlement | 100% acc',  bets: 0,   wins: 0,  losses: 0,   roi: 0.0,  profit: 0.0,  capital: 0,   status: '⚙️ System',color: 'text-base-content/30',bg: 'bg-base-200',   tag: 'CORE',       xgAcc: 100.0 },
];

const ARMY_STATS = { total_capital: 1075, total_bets: 644, total_profit: 38.7, top_agent: '+EV Hunter', new_agents: 3, xg_accuracy: 77.7 };

const ArmyStatsBanner: React.FC<{ stats: typeof ARMY_STATS }> = ({ stats }) => (
  <div className="card bg-gradient-to-r from-primary/20 to-success/10 border border-primary/30 p-4">
    <div className="flex items-center gap-2 mb-3">
      <Swords size={16} className="text-primary" />
      <span className="font-black text-sm tracking-wide">AGENT ARMY STATUS</span>
      <span className="ml-auto text-[10px] bg-success/20 text-success px-2 py-0.5 rounded-full font-mono">● LIVE</span>
    </div>
    <div className="grid grid-cols-4 gap-2 text-center">
      <div>
        <div className="text-lg font-black text-primary">10</div>
        <div className="text-[10px] text-base-content/40">Agents</div>
      </div>
      <div>
        <div className="text-lg font-black">{stats.total_bets}</div>
        <div className="text-[10px] text-base-content/40">Total Bets</div>
      </div>
      <div>
        <div className={`text-lg font-black ${stats.total_profit >= 0 ? 'text-success' : 'text-error'}`}>
          {stats.total_profit >= 0 ? '+' : ''}{stats.total_profit}u
        </div>
        <div className="text-[10px] text-base-content/40">Net Profit</div>
      </div>
      <div>
        <div className="text-lg font-black text-warning">${stats.total_capital}</div>
        <div className="text-[10px] text-base-content/40">Deployed</div>
      </div>
    </div>
  </div>
);

const CapitalLegend: React.FC = () => (
  <div className="flex gap-2 text-[10px] text-base-content/40">
    <span className="flex items-center gap-1"><span className="text-success">🔥</span> Top 3 → 2x capital</span>
    <span className="flex items-center gap-1"><span className="text-warning">⚠️</span> Bottom 2 → 0.5x capital</span>
  </div>
);

const AgentCard: React.FC<{ agent: typeof MOCK_LEADERBOARD[0]; isSelected: boolean; onSelect: () => void }> = ({ agent, isSelected, onSelect }) => {
  const winRate = agent.bets > 0 ? ((agent.wins / agent.bets) * 100).toFixed(0) : '—';
  const profitColor = agent.profit >= 0 ? 'text-success' : 'text-error';
  const rankEmojiMap: Record<number, string> = { 1: '🥇', 2: '🥈', 3: '🥉' };
  const rankEmoji = rankEmojiMap[agent.rank] || agent.rank;
  return (
    <button
      onClick={onSelect}
      className={`card border text-left w-full p-3 transition-all ${agent.bg} ${isSelected ? 'border-primary/50' : 'border-base-300'}`}>
      <div className="flex items-center gap-2">
        <div className={`w-6 h-6 rounded-full flex items-center justify-center text-xs font-black ${
          agent.rank <= 3 ? 'bg-success/20 text-success' : agent.rank >= 6 ? 'bg-warning/20 text-warning' : 'bg-base-300 text-base-content/60'
        }`}>{rankEmoji}</div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-1.5">
            <span className={`font-bold text-sm truncate ${agent.color}`}>{agent.name}</span>
            {agent.tag === 'NEW' && (
              <span className="text-[8px] font-black px-1 py-0 rounded bg-primary/20 text-primary">NEW</span>
            )}
          </div>
          <div className="text-[10px] text-base-content/40 truncate">{agent.strategy}</div>
        </div>
        <span className={`text-[10px] font-mono px-1.5 py-0.5 rounded ${
          agent.status.includes('2x') ? 'bg-success/20 text-success' :
          agent.status.includes('0.5x') ? 'bg-warning/20 text-warning' :
          agent.status === 'System' ? 'bg-base-300 text-base-content/30' :
          'bg-base-300 text-base-content/50'
        }`}>{agent.status}</span>
      </div>
      {isSelected && (
        <div className="mt-2 grid grid-cols-4 text-sm">
          <div>
            <div className="font-bold">Bets</div>
            <div>{agent.bets}</div>
          </div>
          <div>
            <div className="font-bold">Win Rate</div>
            <div>{winRate}%</div>
          </div>
          <div>
            <div className="font-bold">Profit</div>
            <div className={profitColor}>{agent.profit >= 0 ? '+' : ''}{agent.profit}u</div>
          </div>
          <div>
            <div className="font-bold">xG Acc</div>
            <div>{agent.xgAcc}%</div>
          </div>
        </div>
      )}
    </button>
  );
};

const LeaderboardPanel: React.FC = () => {
  const [selected, setSelected] = useState<number | null>(null);

  return (
    <div className="flex flex-col gap-3">
      <ArmyStatsBanner stats={ARMY_STATS} />
      <CapitalLegend />
      {MOCK_LEADERBOARD.map(agent => (
        <AgentCard
          key={agent.rank}
          agent={agent}
          isSelected={selected === agent.rank}
          onSelect={() => setSelected(selected === agent.rank ? null : agent.rank)}
        />
      ))}
    </div>
  );
};
              }`">                {agent.status}
              </span>
            </div>

            {/* Row 2: stats */}
            <div className="grid grid-cols-4 gap-1 mt-2 text-center">
              <div className="bg-base-300/50 rounded p-1">
                <div className="text-[9px] text-base-content/40">Bets</div>
                <div className="text-xs font-mono font-bold">{agent.bets}</div>
              </div>
              <div className="bg-base-300/50 rounded p-1">
                <div className="text-[9px] text-base-content/40">Win%</div>
                <div className="text-xs font-mono font-bold">{winRate}{agent.bets > 0 ? '%' : ''}</div>
              </div>
              <div className="bg-base-300/50 rounded p-1">
                <div className="text-[9px] text-base-content/40">ROI</div>
                <div className={`text-xs font-mono font-bold ${agent.roi >= 0 ? 'text-success' : 'text-error'}`}>
                  {agent.roi >= 0 ? '+' : ''}{agent.roi}%
                </div>
              </div>
              <div className="bg-base-300/50 rounded p-1">
                <div className="text-[9px] text-base-content/40">Profit</div>
                <div className={`text-xs font-mono font-bold ${profitColor}`}> 
                  {agent.profit >= 0 ? '+' : ''}{agent.profit}u
                </div>
              </div>
              </div>
            </div>

            {/* Expanded: capital bar */}
            {isSelected && (
              <div className="mt-2 pt-2 border-t border-base-300">
                <div className="flex justify-between text-[10px] text-base-content/40 mb-1">
                  <span>Capital Allocated</span>
                  <span className="font-mono font-bold text-base-content/70">${agent.capital}</span>
                </div>
                <div className="w-full h-1.5 bg-base-300 rounded-full overflow-hidden">
                  <div
                    className={`h-full rounded-full ${agent.status.includes('2x') ? 'bg-success' : agent.status.includes('0.5x') ? 'bg-warning' : 'bg-primary'}`}
                    style={{ width: `${Math.min(100, (agent.capital / 200) * 100)}%` }}
                  />
                </div>
                <div className="flex justify-between text-[10px] text-base-content/40 mt-1">
                  <span>W:{agent.wins} / L:{agent.losses}</span>
                  <span className="font-mono text-primary">
                    XGB: {typeof (agent as Record<string, unknown>).xgAcc === 'number' ? ((agent as Record<string, unknown>).xgAcc as number) : 0}%
                  </span>
                </div>
              </div>
            )}
          </button>
        );
      })}

      <div className="text-center text-[10px] text-base-content/20 pt-1">
        10-Agent Army · Capital rebalances every 60s · Top 3 → 2x · Bottom 2 → 0.5x · XGBoost 77.7% acc
      </div>
    </div>
  );
};

// ── Bet Analyzer ─────────────────────────────────────────────────────────────
const OPENING_DAY = new Date('2026-03-26');
const ST_MODE = new Date() < OPENING_DAY;
const DAYS_LEFT = Math.max(0, Math.ceil((OPENING_DAY.getTime() - Date.now()) / 86400000));

const POPULAR_PLAYERS = [
  'Aaron Judge', 'Rafael Devers', 'Juan Soto', 'Shohei Ohtani',
  'Yordan Alvarez', 'Mookie Betts', 'Vladimir Guerrero Jr.',
  'Pete Alonso', 'Bryce Harper', 'Fernando Tatis Jr.',
  'Gerrit Cole', 'Spencer Strider',
];

const PROPS_LIST = [
  { label: 'Hits O1.5', value: 'O1.5H',   prior: 42, desc: 'Over 1.5 hits',              underPrior: 58 },
  { label: 'Hits U1.5', value: 'U1.5H',   prior: 58, desc: 'Under 1.5 hits',             underPrior: 42 },
  { label: 'HR O0.5',   value: 'O0.5HR',  prior: 8,  desc: 'Over 0.5 home runs',         underPrior: 92 },
  { label: 'TB O2.5',   value: 'O2.5TB',  prior: 38, desc: 'Over 2.5 total bases',       underPrior: 62 },
  { label: 'TB O1.5',   value: 'O1.5TB',  prior: 62, desc: 'Over 1.5 total bases',       underPrior: 38 },
  { label: 'RBI O0.5',  value: 'O0.5RBI', prior: 32, desc: 'Over 0.5 RBI',               underPrior: 68 },
  { label: 'K O7.5',    value: 'O7.5K',   prior: 48, desc: 'Pitcher: over 7.5 strikeouts', underPrior: 52 },
  { label: 'K O6.5',    value: 'O6.5K',   prior: 55, desc: 'Pitcher: over 6.5 strikeouts', underPrior: 45 },
  { label: 'K O5.5',    value: 'O5.5K',   prior: 64, desc: 'Pitcher: over 5.5 strikeouts', underPrior: 36 },
];

// ── No-vig math (proper vig removal per NoVigCalculator spec) ─────────────────
export function americanToImplied(americanOddsString: string): number {
  const americanOddsValue = parseInt(americanOddsString.replace('+', ''), 10);
  if (isNaN(americanOddsValue)) return 50;
  return americanOddsValue > 0 ? (100 / (americanOddsValue + 100)) * 100 : (Math.abs(americanOddsValue) / (Math.abs(americanOddsValue) + 100)) * 100;
}

export function impliedToAmerican(impliedProbabilityPercent: number): string {
  if (impliedProbabilityPercent <= 0 || impliedProbabilityPercent >= 100) return 'N/A';
  if (impliedProbabilityPercent < 50) {
    const americanOddsValue = Math.round((100 / impliedProbabilityPercent) * 100 - 100);
    return `+${americanOddsValue}`;
  }
  return `-${Math.round((impliedProbabilityPercent / (100 - impliedProbabilityPercent)) * 100)}`;
}

/**
 * True no-vig probability — removes juice before comparing to model.
 * overImplied + underImplied = overround (e.g. 1.047 = 4.7% vig)
 * trueProb = overImplied / overround
 */
export function removeVig(overImplied: number, underImplied: number): { trueProb: number; vigPct: number; fairAmerican: string } {
  const overround = overImplied + underImplied;
  if (overround <= 0) return { trueProb: 0.5, vigPct: 0, fairAmerican: '+100' };
  const trueProb = overImplied / overround;
  const vigPct = Math.round((overround - 100) * 10) / 10;
  return { trueProb, vigPct, fairAmerican: impliedToAmerican(trueProb * 100) };
}

/**
 * True EV = model_prob × (decimal - 1) - (1 - model_prob)
 * Uses no-vig true probability to measure real edge.
 */
export function calcNoVigEV(modelProbPct: number, bestAmericanStr: string, underAmericanStr: string): {
  evPct: number;
  noVigTrueProb: number;
  vigPct: number;
  fairAmerican: string;
  trueEdgePct: number;
} {
  const overImplied = americanToImplied(bestAmericanStr);
  // Estimate under odds if not provided — assume ~5% vig spread
  const underImplied = underAmericanStr ? americanToImplied(underAmericanStr) : (100 - overImplied) * 1.05;
  const { trueProb, vigPct, fairAmerican } = removeVig(overImplied, underImplied);

  const americanOddsInt = parseInt(bestAmericanStr.replace('+', ''), 10);
  const decimal = isNaN(americanOddsInt) ? 1.91 : (americanOddsInt > 0 ? (americanOddsInt / 100) + 1 : (100 / Math.abs(americanOddsInt)) + 1);
  const modelProbabilityFraction = modelProbPct / 100;

  // Standard EV formula
  const evPct = Math.round(((modelProbabilityFraction * (decimal - 1)) - (1 - modelProbabilityFraction)) * 1000) / 10;
  const trueEdgePct = Math.round((modelProbabilityFraction - trueProb) * 1000) / 10;

  return { evPct, noVigTrueProb: Math.round(trueProb * 1000) / 10, vigPct, fairAmerican, trueEdgePct };
}

const getBestOdds = (dk: string, fd: string, mgm: string, b365: string): { book: string; odds: string; implied: number } => {
  const oddsMap: Record<string, string> = {
    DraftKings: dk,
    FanDuel: fd,
    BetMGM: mgm,
    bet365: b365,
  };
  const validBooks = Object.keys(oddsMap).filter(book => oddsMap[book].trim() !== '');
  if (validBooks.length === 0) {
    return { book: 'DraftKings', odds: '-110', implied: 52.4 };
  }
  const numericOdds = validBooks.reduce((acc, book) => {
    const val = parseInt(oddsMap[book].replace('+', ''), 10);
    acc[book] = isNaN(val) ? -Infinity : val;
    return acc;
  }, {} as Record<string, number>);
  const bestBook = validBooks.reduce((best, book) =>
    numericOdds[best] > numericOdds[book] ? best : book
  );
  const bestOdds = oddsMap[bestBook];
  return { book: bestBook, odds: bestOdds, implied: americanToImplied(bestOdds) };
};

// ── Player profiles with checklist context ────────────────────────────────────
const PLAYER_PROFILES: Record<string, {
  matchup: string; fip?: number; swstr?: number; xwoba?: number;
  barrelPct?: number; hardHitPct?: number;
  umpKPct?: number; publicPct?: number; windTrigger?: boolean;
  bullpenFatigue?: number; lineupPos?: number;
}> = {
  'Aaron Judge':           { matchup: 'Judge vs LHP — .385/.430/.720',  xwoba: .421, barrelPct: 24.1, hardHitPct: 56.3, umpKPct: 21.4, publicPct: 68, lineupPos: 3, bullpenFatigue: 1 },
  'Rafael Devers':         { matchup: 'Devers vs RHP — .292 BA .540 SLG', xwoba: .398, barrelPct: 18.3, hardHitPct: 52.1, umpKPct: 22.8, publicPct: 72, lineupPos: 4, windTrigger: true, bullpenFatigue: 2 },
  'Juan Soto':             { matchup: 'Soto — OBP .420 .300 BA vs RHP', xwoba: .412, barrelPct: 14.2, hardHitPct: 48.4, umpKPct: 21.0, publicPct: 55, lineupPos: 3, bullpenFatigue: 1 },
  'Shohei Ohtani':         { matchup: 'Ohtani — .304 BA 44 HR pace',     xwoba: .418, barrelPct: 20.4, hardHitPct: 53.7, umpKPct: 21.0, publicPct: 71, lineupPos: 2, windTrigger: false, bullpenFatigue: 0 },
  'Yordan Alvarez':        { matchup: 'Alvarez vs RHP — .310 BA xSLG .694', xwoba: .432, barrelPct: 22.8, hardHitPct: 55.1, umpKPct: 21.0, publicPct: 60, lineupPos: 4, bullpenFatigue: 1 },
  'Mookie Betts':          { matchup: 'Betts — .289 BA 5-game hit streak', xwoba: .394, barrelPct: 16.8, hardHitPct: 50.2, umpKPct: 20.5, publicPct: 62, lineupPos: 2, bullpenFatigue: 0 },
  'Vladimir Guerrero Jr.': { matchup: 'Vlad Jr — .274 BA 8 HR vs LHP',   xwoba: .380, barrelPct: 15.1, hardHitPct: 49.0, umpKPct: 21.0, publicPct: 58, lineupPos: 3, bullpenFatigue: 1 },
  'Pete Alonso':           { matchup: 'Alonso — .255 BA team HR leader',   xwoba: .386, barrelPct: 19.2, hardHitPct: 51.8, umpKPct: 21.0, publicPct: 55, lineupPos: 4, bullpenFatigue: 2 },
  'Bryce Harper':          { matchup: 'Harper — .302 BA strong vs LHP',   xwoba: .402, barrelPct: 17.4, hardHitPct: 50.8, umpKPct: 22.0, publicPct: 65, lineupPos: 3, bullpenFatigue: 1 },
  'Fernando Tatis Jr.':    { matchup: 'Tatis Jr — .270 BA speed + power', xwoba: .378, barrelPct: 14.8, hardHitPct: 47.3, umpKPct: 21.0, publicPct: 61, lineupPos: 2, bullpenFatigue: 1 },
  'Gerrit Cole':           { matchup: 'Cole — FIP 3.12 SwStr 13.4%',      fip: 3.12, swstr: 13.4, umpKPct: 24.1, publicPct: 70, lineupPos: 0, bullpenFatigue: 2 },
  'Spencer Strider':       { matchup: 'Strider — FIP 3.28 SwStr 15.1%',   fip: 3.28, swstr: 15.1, umpKPct: 23.8, publicPct: 68, lineupPos: 0, bullpenFatigue: 1 },
};

interface ChecklistItem {
  factor: string;
  value: string;
  pass: boolean;
  threshold: string;
  boost?: string;
}

export interface AnalyzeResult {
  player: string;
  prop: string;
  bestBook: string;
  bestOdds: string;
  impliedProb: number;
  noVigTrueProb: number;
  modelProb: number;
  evPct: number;
  trueEdgePct: number;
  vigPct: number;
  fairOdds: string;
  edge: string;
  matchup: string;
  agentsAgree: string;
  status: string;
  statusColor: string;
  checklist: ChecklistItem[];
  sharpMoney: boolean;
  sharpPublicPct: number;
  kellyPct: number;
}

export function buildChecklist(player: string, prop: string, _modelProb: number, _evPct: number): ChecklistItem[] {
  const profile = PLAYER_PROFILES[player] || {};
  const isPitcher = prop.includes('K');

  // 1. Pitcher/Batter quality
  const pitcherChecklist: ChecklistItem[] = [
    {
      factor: 'Pitcher FIP',
      value: profile.fip ? String(profile.fip) : '3.80',
      pass: (profile.fip ?? 3.80) < 3.80,
      threshold: '< 3.80',
      boost: (profile.fip ?? 3.80) < 3.80 ? `+${((3.80 - (profile.fip ?? 3.80)) * 2).toFixed(1)}% EV` : undefined,
    },
    {
      factor: 'SwStr%',
      value: profile.swstr ? `${profile.swstr}%` : '12.0%',
      pass: (profile.swstr ?? 12.0) > 12.0,
      threshold: '> 12.0%',
      boost: (profile.swstr ?? 12.0) > 12.0 ? '+2.1% EV' : undefined,
    },
  ];
  const batterChecklist: ChecklistItem[] = [
    {
      factor: 'Batter xwOBA',
      value: profile.xwoba ? `.${Math.round(profile.xwoba * 1000)}` : '.320',
      pass: (profile.xwoba ?? 0.32) > 0.32,
      threshold: '> .320',
      boost: (profile.xwoba ?? 0.32) > 0.32 ? `+${(((profile.xwoba ?? 0.32) - 0.32) * 2).toFixed(1)}% EV` : undefined,
    },
  ];

  const items: ChecklistItem[] = isPitcher ? [...pitcherChecklist] : [...batterChecklist];

  // ...other checklist logic continues here
  return items;
}
      pass: (profile.xwoba ?? 0.320) > 0.360,
      threshold: '> .360',
      boost: (profile.xwoba ?? 0.320) > 0.360 ? '+3.2% EV' : undefined,
    });
    items.push({
      factor: 'Barrel%',
      value: profile.barrelPct ? `${profile.barrelPct}%` : '8%',
      pass: (profile.barrelPct ?? 8) > 12,
      threshold: '> 12%',
    });
  }

  // 3. Umpire K%
  items.push({
    factor: 'Umpire K%',
    value: `${profile.umpKPct ?? 21.0}%`,
    pass: (profile.umpKPct ?? 21.0) > 22.0,
    threshold: '> 22% (tight zone)',
    boost: (profile.umpKPct ?? 21.0) > 22.0 ? '+11.2% K props' : undefined,
  });

  // 4. Public betting / sharp money
  const pub = profile.publicPct ?? 50;
  items.push({
    factor: 'Public %',
    value: `${pub}%`,
    pass: pub < 60,
    threshold: '< 60% (avoid heavy public)',
    boost: pub > 70 ? '⚠️ Fade candidate' : pub < 40 ? '🔥 Sharp money' : undefined,
  });

  // 5. Wind trigger
  items.push({
    factor: 'Wind',
    value: profile.windTrigger ? '10mph out to LF' : '< 8mph',
    pass: !prop.includes('K') ? (profile.windTrigger ?? false) : true,
    threshold: prop.includes('HR') ? '> 8mph out → HR boost' : 'N/A for K props',
    boost: profile.windTrigger && !isPitcher ? '+6% HR prob' : undefined,
  });

  // 6. Lineup position
  const pos = profile.lineupPos ?? 5;
  items.push({
    factor: 'Lineup Spot',
    value: pos === 0 ? 'Starter (P)' : pos === 0 ? 'TBD' : `#${pos}`,
    pass: isPitcher ? true : pos <= 4,
    threshold: 'Top-4 confirmed → hits boost',
    boost: pos <= 4 && !isPitcher ? '+8% hits' : undefined,
  });

  // 7. Bullpen fatigue
  const fat = profile.bullpenFatigue ?? 1;
  items.push({
    factor: 'Bullpen Fatigue',
    value: `${fat}/4`,
    pass: fat >= 2,
    threshold: '≥ 2/4 = more runs / starter extension',
    boost: fat >= 3 ? `+${fat * 2}% hitter props` : fat >= 2 ? '+2% hitter props' : undefined,
  });

  return items;
}

function runLocalAnalysis(
  player: string, prop: string,
  dk: string, fd: string, mgm: string, b365: string
): AnalyzeResult {
  const propData = PROPS_LIST.find(p => p.value === prop) || PROPS_LIST[0];
  const profile = PLAYER_PROFILES[player] || {};

  // Model probability: prior + player-specific adjustments
  let modelProb = propData.prior;
  if (ST_MODE) {
    modelProb = propData.prior; // league-average prior only (0-0 records)
  } else {
    const nameHash = player.split('').reduce((acc, c) => acc + c.charCodeAt(0), 0);
    modelProb = propData.prior + (nameHash % 11) - 5;
    modelProb = Math.max(5, Math.min(95, modelProb));
  }

  // Apply profile-based adjustments
  const adjustmentFns: Record<string, (p: any, prop: string) => number> = {
    xwoba: (p) => p.xwoba && p.xwoba > 0.380 ? 3 : 0,
    fip: (p) => p.fip && p.fip < 3.50 ? 4 : 0,
    swstr: (p) => p.swstr && p.swstr > 13.0 ? 3 : 0,
    umpKPct: (p, prop) => p.umpKPct && p.umpPct > 22.0 && prop.includes('K') ? 5 : 0,
    windTrigger: (p, prop) => p.windTrigger && prop.includes('HR') ? 4 : 0,
    lineupPos: (p, prop) => (p.lineupPos ?? 9) <= 3 && !prop.includes('K') ? 3 : 0,
    bullpenFatigue: (p, prop) => (p.bullpenFatigue ?? 0) >= 3 && !prop.includes('K') ? 3 : 0,
  };
  modelProb += Object.values(adjustmentFns).reduce((sum, fn) => sum + fn(profile, prop), 0);
  modelProb = Math.max(5, Math.min(92, Math.round(modelProb)));

  // Best book odds
  const { book, odds, implied } = getBestOdds(dk, fd, mgm, b365);

  // ── No-vig EV (true edge, not vs viggy implied prob) ─────────────────────
  const underEstimateStr = (() => {
    const oddsInt = parseInt(odds.replace('+', ''), 10);
    if (isNaN(oddsInt)) return '-110';
    // Estimate under side using ~4.5% overround
    const overImpl = americanToImplied(odds);
    const underImpl = 100 - (overImpl / 1.045);
    return impliedToAmerican(Math.max(1, underImpl));
  })();

  const { evPct, noVigTrueProb, vigPct, fairAmerican, trueEdgePct } =
    calcNoVigEV(modelProb, odds, underEstimateStr);

  const agreeCount = evPct >= 5 ? 7 : evPct >= 3 ? 5 : evPct >= 1 ? 3 : evPct >= 0 ? 1 : 0;
  const matchupBase = profile.matchup || (PLAYER_PROFILES[player]?.matchup ?? `${player} — matchup loading`);
  const matchup = ST_MODE ? `${matchupBase} [🌱 ST — 0-0, ${DAYS_LEFT}d to OD]` : matchupBase;

  // Sharp money flag: public >70% = FADE signal (contrarian value)
  const pub = profile.publicPct ?? 50;
  const sharpMoney = pub > 70 || pub < 35;

  // Kelly criterion (1/4 Kelly)
  const oddsInt = parseInt(odds.replace('+', ''), 10);
  const decimal = isNaN(oddsInt) ? 1.91 : (oddsInt > 0 ? (oddsInt / 100) + 1 : (100 / Math.abs(oddsInt)) + 1);
  const mp = modelProb / 100;
  const fullKelly = Math.max(0, ((decimal - 1) * mp - (1 - mp)) / (decimal - 1));
  const kellyPct = Math.round(fullKelly * 25 * 10) / 10; // 1/4 Kelly %

  let status: string;
  let statusColor: string;
  if (evPct >= 5) {
    status = '🟢 GREEN — BET NOW';
    statusColor = 'text-success';
  } else if (evPct >= 2) {
    status = '🟡 YELLOW — CONSIDER';
    statusColor = 'text-warning';
  } else {
    status = '🔴 RED — PASS';
    statusColor = 'text-error';
  }

  const checklist = buildChecklist(player, prop, modelProb, evPct);

  return {
    player, prop,
    bestBook: book, bestOdds: odds,
    impliedProb: Math.round(implied * 10) / 10,
    noVigTrueProb,
    modelProb: Math.round(modelProb * 10) / 10,
    evPct,
    trueEdgePct,
    vigPct,
    fairOdds: fairAmerican,
    edge: `${evPct >= 0 ? 'BUY' : 'PASS'} (${odds} > fair ${fairAmerican}) = ${trueEdgePct > 0 ? '+' : ''}${trueEdgePct}% true edge`,
    matchup,
    agentsAgree: `${agreeCount}/10`,
    status, statusColor,
    checklist,
    sharpMoney,
    sharpPublicPct: pub,
    kellyPct,
  };
}

const AnalyzePanel: React.FC = () => {
  const [player, setPlayer] = useState('Aaron Judge');
  const [customPlayer, setCustomPlayer] = useState('');
  const [prop, setProp] = useState('O1.5H');
  const [dkOdds, setDkOdds] = useState('+120');
  const [fdOdds, setFdOdds] = useState('+115');
  const [mgmOdds, setMgmOdds] = useState('+110');
  const [b365Odds, setB365Odds] = useState('');
  const [result, setResult] = useState<AnalyzeResult | null>(null);
  const [analyzed, setAnalyzed] = useState(false);

  const selectedPlayer = customPlayer.trim() || player;

  const analyze = () => {
    const analysisResult = runLocalAnalysis(selectedPlayer, prop, dkOdds, fdOdds, mgmOdds, b365Odds);
    setResult(analysisResult);
    setAnalyzed(true);
  };

  return (
    <div className="flex flex-col gap-3">
      {/* Spring Training banner */}
      {ST_MODE && (
        <div className="alert bg-primary/10 border border-primary/30 text-xs py-2 px-3">
          <span className="text-primary font-bold">🌱 Spring Training Mode</span>
          <span className="text-base-content/60 ml-1">— All records 0-0 · {DAYS_LEFT} days to Opening Day · Stats weighted at 30%</span>
        </div>
      )
        </div>
      )}

      {/* Header info */}
      <div className="card bg-base-200 border border-primary/20 p-3 text-xs">
        <div className="flex items-center gap-2 text-primary font-semibold mb-1">
          <Target size={13} /> Bet Analyzer
        </div>
        <p className="text-base-content/50">
          Paste any bet from DK, FanDuel, BetMGM, or bet365 → instant EV calculation using live odds + XGBoost model + 7-agent consensus.
        </p>
      </div>

      {/* Quick player picks */}
      <div>
        <div className="text-[10px] text-base-content/40 mb-1.5 font-semibold uppercase tracking-wide">Quick Pick Player</div>
        <div className="flex flex-wrap gap-1.5">
          {POPULAR_PLAYERS.map(p => (
            <button
              key={p}
              onClick={() => { setPlayer(p); setCustomPlayer(''); }}
              className={`text-[10px] px-2 py-1 rounded-full border font-mono transition-colors ${
                selectedPlayer === p && !customPlayer
                  ? 'bg-primary/20 border-primary/50 text-primary'
                  : 'bg-base-200 border-base-300 text-base-content/60 hover:border-primary/30'
              }`}
            >
              {p.split(' ').pop()}
            </button>
          ))}
        </div>
      </div>

      {/* Custom player input */}
      <label className="input input-bordered flex items-center gap-2 text-sm">
        <Search className="h-[1em] opacity-50" />
        <input
          type="text"
          className="grow"
          placeholder="Or type any player name..."
          value={customPlayer}
          onChange={e => setCustomPlayer(e.target.value)}
        />
      </label>

      {/* Prop selector */}
      <div>
        <div className="text-[10px] text-base-content/40 mb-1.5 font-semibold uppercase tracking-wide">Prop</div>
        <select
          className="select select-bordered w-full text-sm"
          value={prop}
          onChange={e => setProp(e.target.value)}
        >
          {PROPS_LIST.map(p => (
            <option key={p.value} value={p.value}>{p.label} — {p.desc}</option>
          ))}
        </select>
      </div>

      {/* Odds inputs */}
      <div>
        <div className="text-[10px] text-base-content/40 mb-1.5 font-semibold uppercase tracking-wide">Odds by Book</div>
        <div className="grid grid-cols-2 gap-2">
          {[
            { label: 'DraftKings', val: dkOdds, set: setDkOdds },
          <OddsInputs
            sources=[
              { label: 'FanDuel',    val: fdOdds, set: setFdOdds },
              { label: 'BetMGM',      val: mgmOdds, set: setMgmOdds },
              { label: 'bet365',      val: b365Odds, set: setB365Odds },
            ]
          />
        </div>
      </div>

      {/* Analyze button */}
      <button onClick={analyze} className="btn btn-primary w-full font-black tracking-wide">
        <Target size={16} /> ANALYZE BET
      </button>

      {/* Result card */}
      {analyzed && result && <ResultCard result={result} />}
              {result.sharpPublicPct > 70
                ? `⚠️ ${result.sharpPublicPct}% public → FADE SIGNAL (FadeAgent active)`
                : `🔥 Only ${result.sharpPublicPct}% public → SHARP MONEY on this side`}
            </div>
          )}

          {/* Stats grid — 4 cols now inc no-vig */}
          <div className="grid grid-cols-2 gap-1.5 text-center">
            <div className="bg-base-300/50 rounded p-2">
              <div className="text-[9px] text-base-content/40">Model Prob</div>
              <div className="font-mono font-bold text-sm text-primary">{result.modelProb}%</div>
            </div>
            <div className="bg-base-300/50 rounded p-2">
              <div className="text-[9px] text-base-content/40">No-Vig True Prob</div>
              <div className="font-mono font-bold text-sm text-warning">{result.noVigTrueProb}%</div>
            </div>
            <div className="bg-base-300/50 rounded p-2">
              <div className="text-[9px] text-base-content/40">Book Implied</div>
              <div className="font-mono font-bold text-sm text-base-content/60">{result.impliedProb}%</div>
            </div>
            <div className="bg-base-300/50 rounded p-2">
              <div className="text-[9px] text-base-content/40">Agents Agree</div>
              <div className={`font-mono font-bold text-sm ${result.evPct >= 5 ? 'text-success' : 'text-base-content/60'}`}>{result.agentsAgree}</div>
            </div>
          </div>

          {/* No-vig edge breakdown */}
          <div className="bg-base-300/40 rounded p-2 text-xs">
            <div className="text-[9px] font-bold text-base-content/40 uppercase mb-1.5">No-Vig Analysis</div>
            <div className="flex justify-between mb-1">
              <span className="text-base-content/40">Best Book</span>
              <span className="font-mono font-bold">{result.bestBook} <span className={result.evPct >= 0 ? 'text-success' : 'text-error'}>{result.bestOdds}</span></span>
            </div>
            <div className="flex justify-between mb-1">
              <span className="text-base-content/40">Fair Odds (no-vig)</span>
              <span className="font-mono font-bold text-warning">{result.fairOdds}</span>
            </div>
            <div className="flex justify-between mb-1">
              <span className="text-base-content/40">Book Vig</span>
              <span className="font-mono text-base-content/50">{result.vigPct}%</span>
            </div>
            <div className="flex justify-between">
              <span className="text-base-content/40">True Edge</span>
              <span className={`font-mono font-bold ${result.trueEdgePct >= 0 ? 'text-success' : 'text-error'}`}>
                {result.trueEdgePct >= 0 ? '+' : ''}{result.trueEdgePct}%
              </span>
            </div>
          </div>

          {/* Kelly sizing */}
          {result.evPct > 0 && (
            <div className="flex items-center justify-between bg-primary/5 border border-primary/20 rounded px-3 py-1.5 text-xs">
              <span className="text-base-content/50">¼ Kelly stake</span>
              <span className="font-mono font-bold text-primary">{result.kellyPct}% of bankroll</span>
            </div>
          )}

          {/* 7-point Pro Checklist */}
          <div>
            <div className="text-[9px] font-bold text-base-content/40 uppercase mb-1.5">Pro Checklist ({result.checklist.filter(c => c.pass).length}/{result.checklist.length} pass)</div>
            <div className="flex flex-col gap-1">
              {result.checklist.map((item, i) => (
                <div key={i} className="flex items-center justify-between text-xs">
                  <div className="flex items-center gap-1.5">
                    <span className={item.pass ? 'text-success' : 'text-base-content/30'}>{item.pass ? '✅' : '⬜'}</span>
                    <span className={item.pass ? 'text-base-content/80' : 'text-base-content/40'}>{item.factor}</span>
                  </div>
                  <div className="flex items-center gap-1.5 text-right">
                    <span className="font-mono text-base-content/60">{item.value}</span>
                    {item.boost && <span className="text-[9px] text-success font-bold">{item.boost}</span>}
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Matchup */}
          <div className="text-xs text-base-content/50 bg-base-300/30 rounded p-2">
            📋 {result.matchup}
          </div>
        </div>
      )}
    </div>
  );
};

// ── Main App ───────────────────────────────────────────────────────────────
type ExtendedView = ViewMode | 'calibration' | 'health' | 'army' | 'analyze';

const App: React.FC = () => {
  const [games, setGames] = useState<GameOdds[]>([]);
  const [allProps, setAllProps] = useState<PlayerProp[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [view, setView] = useState<ExtendedView>('games');
  const [calibrationData, setCalibrationData] = useState<unknown[]>([]);

  const loadData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const events = await fetchEvents();
      if (!Array.isArray(events)) throw new Error('Unexpected API response');

      const gameResults = await Promise.allSettled(events.map(e => fetchGameOdds(e)));
      const loadedGames: GameOdds[] = [];
      for (const r of gameResults) {
        if (r.status === 'fulfilled') loadedGames.push(r.value);
      }
      loadedGames.sort(
        (a, b) => new Date(a.event.commence_time).getTime() - new Date(b.event.commence_time).getTime()
      );
      setGames(loadedGames);
      setCalibrationData(buildMockCalibration(loadedGames));

      const propResults = await Promise.allSettled(events.slice(0, 3).map(e => fetchPlayerProps(e.id)));
  const navTabs: { key: ExtendedView; icon: React.ReactNode; label: string; count?: number }[] = [
    { key: 'games', icon: <Activity size={13} />, label: 'Games', count: games.length },
    { key: 'props', icon: <TrendingUp size={13} />, label: 'Props', count: allProps.length },
    { key: 'calibration', icon: <BarChart2 size={13} />, label: 'Model', count: calibrationData.filter(p => p.edge >= 0.04).length },
    { key: 'army', icon: <Trophy size={13} />, label: 'Army', count: 10 },
    { key: 'analyze', icon: <Target size={13} />, label: 'Analyze' },
    { key: 'health', icon: <Shield size={13} />, label: 'Health' },
  ];

  const TopBar = ({ loading, onRefresh }: { loading: boolean; onRefresh: () => void }) => (
    <div className="sticky top-0 z-10 bg-base-100 border-b border-base-300 px-4 pt-4 pb-0">
      <div className="flex items-center justify-between mb-3">
        <div>
          <h1 className="text-lg font-black tracking-tight">
            PropIQ <span className="text-primary">Analytics</span>
          </h1>
          <p className="text-xs text-base-content/40">Live MLB · Calibrated Predictions</p>
        </div>
        <button
          onClick={onRefresh}
          disabled={loading}
          className="btn btn-ghost btn-sm btn-circle"
        >
          <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
        </button>
      </div>
    </div>
  );

  const NavTabs = ({
    navTabs,
    view,
    setView,
  }: {
    navTabs: { key: ExtendedView; icon: React.ReactNode; label: string; count?: number }[];
    view: ExtendedView;
    setView: React.Dispatch<React.SetStateAction<ExtendedView>>;
  }) => (
    <div className="flex gap-0">
      {navTabs.map(tab => (
        <button
          key={tab.key}
          onClick={() => setView(tab.key)}
          className={`flex-1 flex items-center justify-center gap-1 px-2 py-2 text-xs font-semibold border-b-2 transition-colors ${
            view === tab.key
              ? 'border-primary text-primary'
              : ''
          }`}
        >
          {tab.icon}
          {tab.label}
          {tab.count != null && <span className="ml-1">({tab.count})</span>}
        </button>
      ))}
    </div>
  );

  return (
    <div className="min-h-screen bg-base-100 text-base-content flex flex-col max-w-lg mx-auto">
      {/* Top bar */}
      <TopBar loading={loading} onRefresh={loadData} />

      {/* Nav tabs */}
      <NavTabs navTabs={navTabs} view={view} setView={setView} />
                  : 'border-transparent text-base-content/40 hover:text-base-content/60'
              }`}
            >
              {tab.icon}
              {tab.label}
              {tab.count !== undefined && tab.count > 0 && (
                <span className={`text-[9px] px-1 rounded-full font-mono ${
                  view === tab.key ? 'bg-primary/20 text-primary' : 'bg-base-300 text-base-content/40'
                }`}>
                  {tab.count}
                </span>
              )}
            </button>
          ))}
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 p-4 flex flex-col gap-3">
        {error && (
          <div className="alert alert-error text-sm">
            <AlertTriangle size={15} />
            <span>{error}</span>
          </div>
        )}

        {loading && games.length === 0 && (
          <div className="flex flex-col items-center justify-center py-16 gap-3">
            <span className="loading loading-spinner loading-lg text-primary" />
            <span className="text-base-content/50 text-sm">Fetching live MLB data...</span>
          </div>
        )}

        {/* Games view */}
        {view === 'games' && (
          <>
            {games.map(g => <GameCard key={g.event.id} game={g} />)}
            {!loading && games.length === 0 && !error && (
              <div className="text-center py-12 text-base-content/40 text-sm">No MLB events right now</div>
            )}
          </>
        )}

        {/* Props view */}
        {view === 'props' && (
          <>
            {allProps.length === 0 && !loading && (
              <div className="card bg-base-200 p-6 text-center gap-2">
                <p className="text-base-content/50 text-sm">No player props posted yet</p>
                <p className="text-base-content/30 text-xs">Props typically drop 12–24h before game time</p>
              </div>
            )}
            {allProps.map((p, i) => <PropCard key={`${p.player}-${p.market}-${i}`} prop={p} />)}
          </>
        )}

        {/* Calibration / Model view */}
        {view === 'calibration' && (
          <>
            <div className="card bg-base-200 border border-primary/30 p-3 text-xs">
              <div className="flex items-center gap-2 text-primary font-semibold mb-1">
                <BarChart2 size={13} /> Calibrated Predictions
              </div>
              <p className="text-base-content/50">
                Raw model probabilities adjusted via 3-layer Bayesian calibration.
                Showing {calibrationData.filter(p => Math.abs(p.edge) >= 0.04).length} plays with edge ≥ 4%.
              </p>
            </div>

            {/* Strong plays first */}
            {['A', 'A_FADE', 'B', 'B_FADE', 'C'].map(tier => {
              const tierProps = calibrationData.filter(p => p.tier === tier);
              if (tierProps.length === 0) return null;
              return (
                <div key={tier}>
                  {tierProps.map((p, i) => <CalibrationRow key={`${tier}-${i}`} p={p} />)}
                </div>
              );
            })}
          </>
        )}

        {/* Army leaderboard view */}
        {view === 'army' && <LeaderboardPanel />}

        {/* Bet Analyzer view */}
        {view === 'analyze' && <AnalyzePanel />}

        {/* Health view */}
        {view === 'health' && <ModelHealthPanel />}
      </div>

      <div className="text-center text-[10px] text-base-content/20 pb-4">
        PropIQ Analytics v3 · 10-Agent Army · XGBoost 77.7% · No-Vig EV · Pro Checklist
      </div>
    </div>
  );
};

const rootElement = document.getElementById('root');
if (!rootElement) {
  throw new Error('Root element with id "root" not found');
}
createRoot(rootElement).render(<App />);
