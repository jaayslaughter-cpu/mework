import { OddsEvent, GameOdds, PlayerProp } from '../types';

const API_KEY = 'e4e30098807a9eece674d85e30471f03';
const BASE = 'https://api.the-odds-api.com/v4/sports/baseball_mlb';

const PROP_MARKETS = [
  'pitcher_strikeouts',
  'batter_home_runs',
  'batter_hits',
  'batter_total_bases',
  'batter_rbis',
  'batter_runs_scored',
  'batter_singles',
  'batter_doubles',
  'batter_walks',
];

const MARKET_LABELS: Record<string, string> = {
  pitcher_strikeouts: 'Strikeouts',
  batter_home_runs: 'Home Runs',
  batter_hits: 'Hits',
  batter_total_bases: 'Total Bases',
  batter_rbis: 'RBIs',
  batter_runs_scored: 'Runs Scored',
  batter_singles: 'Singles',
  batter_doubles: 'Doubles',
  batter_walks: 'Walks',
};

async function curlJson<T>(url: string): Promise<T> {
  const res = await window.tasklet.runCommand(
    `curl -s "${url}"`,
    30
  );
  try {
    return JSON.parse(res.log) as T;
  } catch {
    console.error('Failed to parse API response:', res.log);
    throw new Error('Failed to parse API response');
  }
}

export async function fetchEvents(): Promise<OddsEvent[]> {
  return curlJson<OddsEvent[]>(`${BASE}/events?apiKey=${API_KEY}`);
}

export async function fetchGameOdds(event: OddsEvent): Promise<GameOdds> {
  const data = await curlJson<OddsEvent>(
    `${BASE}/events/${event.id}/odds?apiKey=${API_KEY}&regions=us&markets=h2h,spreads,totals`
  );

  let moneyline: GameOdds['moneyline'] = null;
  let spread: GameOdds['spread'] = null;
  let total: GameOdds['total'] = null;

  for (const bm of data.bookmakers || []) {
    for (const mkt of bm.markets) {
      if (mkt.key === 'h2h' && !moneyline) {
        const home = mkt.outcomes.find(o => o.name === event.home_team);
        const away = mkt.outcomes.find(o => o.name === event.away_team);
        if (home && away) {
          moneyline = { home: home.price, away: away.price, book: bm.title };
        }
      }
      if (mkt.key === 'spreads' && !spread) {
        const home = mkt.outcomes.find(o => o.name === event.home_team);
        const away = mkt.outcomes.find(o => o.name === event.away_team);
        if (home && away) {
          spread = { home: home.price, away: away.price, line: home.point ?? 0, book: bm.title };
        }
      }
      if (mkt.key === 'totals' && !total) {
        const over = mkt.outcomes.find(o => o.name === 'Over');
        const under = mkt.outcomes.find(o => o.name === 'Under');
        if (over && under) {
          total = { over: over.price, under: under.price, line: over.point ?? 0, book: bm.title };
        }
      }
    }
  }

  return { event, moneyline, spread, total, props: [] };
}

export async function fetchPlayerProps(eventId: string): Promise<PlayerProp[]> {
  const marketsParam = PROP_MARKETS.join(',');
  const data = await curlJson<OddsEvent>(
    `${BASE}/events/${eventId}/odds?apiKey=${API_KEY}&regions=us&markets=${marketsParam}`
  );

  const props: PlayerProp[] = [];
  for (const bm of data.bookmakers || []) {
    for (const mkt of bm.markets) {
      const label = MARKET_LABELS[mkt.key] || mkt.key;
      // Group outcomes by player description
      const playerMap = new Map<string, { over?: number; under?: number; point?: number }>();
      for (const o of mkt.outcomes) {
        const player = o.description || 'Unknown';
        if (!playerMap.has(player)) playerMap.set(player, {});
        const entry = playerMap.get(player)!;
        if (o.name === 'Over') { entry.over = o.price; entry.point = o.point; }
        if (o.name === 'Under') { entry.under = o.price; }
      }
      for (const [player, data] of playerMap) {
        if (data.over != null && data.under != null && data.point != null) {
          props.push({
            player,
            market: mkt.key,
            marketLabel: label,
            line: data.point,
            overPrice: data.over,
            underPrice: data.under,
            bookmaker: bm.title,
          });
        }
      }
    }
  }
  return props;
}

export function americanOdds(decimal: number): string {
  if (decimal >= 2) {
    return '+' + Math.round((decimal - 1) * 100);
  }
  return '-' + Math.round(100 / (decimal - 1));
}

export function formatTime(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    timeZoneName: 'short',
  });
}
