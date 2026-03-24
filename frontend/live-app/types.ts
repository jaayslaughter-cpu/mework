export interface OddsEvent {
  id: string;
  sport_key: string;
  sport_title: string;
  commence_time: string;
  home_team: string;
  away_team: string;
  bookmakers: Bookmaker[];
}

export interface Bookmaker {
  key: string;
  title: string;
  markets: Market[];
}

export interface Market {
  key: string;
  outcomes: Outcome[];
}

export interface Outcome {
  name: string;
  price: number;
  point?: number;
  description?: string;
}

export interface PlayerProp {
  player: string;
  market: string;
  marketLabel: string;
  line: number;
  overPrice: number;
  underPrice: number;
  bookmaker: string;
}

export interface GameOdds {
  event: OddsEvent;
  moneyline: { home: number; away: number; book: string } | null;
  spread: { home: number; away: number; line: number; book: string } | null;
  total: { over: number; under: number; line: number; book: string } | null;
  props: PlayerProp[];
}

export type ViewMode = 'games' | 'props';
