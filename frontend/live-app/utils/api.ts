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

export async function curlJson<T>(url: string): Promise<T> {
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

export function fetchEvents(): Promise<OddsEvent[]> {
  return curlJson<OddsEvent[]>(`${BASE}/events?apiKey=${API_KEY}`);
}

export async function fetchGameOdds(event: OddsEvent): Promise<GameOdds> {
  const data = await curlJson<OddsEvent>(
    `${BASE}/events/${event.id}/odds?apiKey=${API_KEY}&regions=us&markets=h2h,spreads,totals`
  );

  const result: {
    moneyline: GameOdds['moneyline'];
    spread: GameOdds['spread'];
    total: GameOdds['total'];
  } = { moneyline: null, spread: null, total: null };

  const keyMap = {
    h2h: 'moneyline',
    spreads: 'spread',
    totals: 'total',
  } as const;

  const handlers: Record<string, (mkt: any, bm: any) => any> = {
    h2h: (mkt, bm) => {
      const home = mkt.outcomes.find((o: any) => o.name === event.home_team);
      const away = mkt.outcomes.find((o: any) => o.name === event.away_team);
      if (home && away) {
        return { home: home.price, away: away.price, book: bm.title };
      }
    },
    spreads: (mkt, bm) => {
      const home = mkt.outcomes.find((o: any) => o.name === event.home_team);
      const away = mkt.outcomes.find((o: { name: string; point?: number; price: number }) => o.name === event.away_team);
      if (home && away) {
        return {
          home: { point: home.point, price: home.price },
          away: { point: away.point, price: away.price },
          book: bm.title,
        };
      }
      return null;
    },
    totals: (mkt, bm) => {
      const over = mkt.outcomes.find((o: { name: string; point?: number; price: number }) => o.name === 'Over');
      const under = mkt.outcomes.find((o: { name: string; point?: number; price: number }) => o.name === 'Under');
      if (over && under) {
        return {
          over: { total: over.point, price: over.price },
          under: { total: under.point, price: under.price },
          book: bm.title,
        };
      spread: (mkt, bm) => {
      }
      return null;
    },
  };

  for (const bm of data.bookmakers) {
    for (const mkt of bm.markets) {
      const mapKey = keyMap[mkt.key as keyof typeof keyMap];
      const handler = handlers[mkt.key];
      if (mapKey && handler) {
        result[mapKey] = handler(mkt, bm);
      }
    }
  }

  return result;
}
    moneyline: (mkt, bm) => {
      const home = mkt.outcomes.find((o: { name: string; point?: number; price: number }) => o.name === event.home_team);
      const away = mkt.outcomes.find((o: { name: string; point?: number; price: number }) => o.name === event.away_team);
      if (home && away) {
        return { home: home.price, away: away.price, line: home.point ?? 0, book: bm.title };
      }
      return null;
    },
    totals: (mkt, bm) => {
      const over = mkt.outcomes.find((o: { name: string; point?: number; price: number }) => o.name === 'Over');
      const under = mkt.outcomes.find((o: { name: string; point?: number; price: number }) => o.name === 'Under');
      if (over && under) {
        return { over: over.price, under: under.price, line: over.point ?? 0, book: bm.title };
      }
      return null;
    },
  };

  for (const bm of data.bookmakers || []) {
    for (const mkt of bm.markets) {
      const prop = keyMap[mkt.key];
      if (prop && result[prop] === null) {
        const value = handlers[mkt.key]?.(mkt, bm);
        if (value) {
          result[prop] = value;
        }
      }
    }
    if (result.moneyline && result.spread && result.total) {
      break;
    }
  }

  return { event, moneyline: result.moneyline, spread: result.spread, total: result.total, props: [] };
}

export async function fetchPlayerProps(eventId: string): Promise<PlayerProp[]> {
  const marketsParam = PROP_MARKETS.join(',');
  const data = await curlJson<OddsEvent>(
    `${BASE}/events/${eventId}/odds?apiKey=${API_KEY}&regions=us&markets=${marketsParam}`
  );

  const props: PlayerProp[] = [];
  const outcomeMapping: Record<string, { priceKey: 'over' | 'under'; pointKey?: 'point' }> = {
    Over: { priceKey: 'over', pointKey: 'point' },
    Under: { priceKey: 'under' },
  };

  for (const bm of data.bookmakers || []) {
    for (const mkt of bm.markets) {
      const label = MARKET_LABELS[mkt.key] || mkt.key;
      // Group outcomes by player description
      const playerMap = new Map<string, { over?: number; under?: number; point?: number }>();
      for (const o of mkt.outcomes) {
        const player = o.description || 'Unknown';
        const entry = playerMap.get(player) || {};
        playerMap.set(player, entry);
        const mapping = outcomeMapping[o.name];
        if (mapping) {
          entry[mapping.priceKey] = o.price;
          if (mapping.pointKey) {
            entry.point = o.point;
          }
        }
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
    return `+${Math.round((decimal - 1) * 100)}`;
  }
  return `-${Math.round(100 / (decimal - 1))}`;
}

export function formatTime(iso: string): string {
  const date = new Date(iso);
  return date.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    timeZoneName: 'short',
  });
}
