import React, { useState } from 'react';
import { ChevronDown, ChevronUp, Clock, TrendingUp } from 'lucide-react';
import { GameOdds, PlayerProp } from '../types';
import { americanOdds, formatTime, fetchPlayerProps } from '../utils/api';
import { PropCard } from './PropCard';

interface GameCardProps {
  game: GameOdds;
}

export const GameCard: React.FC<GameCardProps> = ({ game }) => {
  const [expanded, setExpanded] = useState(false);
  const [props, setProps] = useState<PlayerProp[]>(game.props);
  const [loadingProps, setLoadingProps] = useState(false);

  const handleExpand = async () => {
    const next = !expanded;
    setExpanded(next);
    if (next && props.length === 0 && !loadingProps) {
      setLoadingProps(true);
      try {
        const fetched = await fetchPlayerProps(game.event.id);
        setProps(fetched);
      } catch (e) {
        console.error('Failed to fetch props:', e);
      } finally {
        setLoadingProps(false);
      }
    }
  };

  const { event, moneyline, spread, total } = game;

  return (
    <div className="card bg-base-200 shadow-md">
      <div className="card-body p-4 gap-3">
        {/* Teams row */}
        <div className="flex items-center justify-between">
          <div className="flex flex-col gap-1">
            <span className="font-semibold text-sm">{event.away_team}</span>
            <span className="text-base-content/50 text-xs">@</span>
            <span className="font-semibold text-sm">{event.home_team}</span>
          </div>
          <div className="flex items-center gap-1 text-base-content/50 text-xs">
            <Clock size={12} />
            {formatTime(event.commence_time)}
          </div>
        </div>

        {/* Odds grid */}
        <div className="grid grid-cols-3 gap-2">
          {/* Moneyline */}
          <div className="flex flex-col items-center bg-base-300 rounded-lg p-2">
            <span className="text-[10px] uppercase tracking-wider text-base-content/40 mb-1">ML</span>
            {moneyline ? (
              <>
                <span className={`text-sm font-mono font-bold ${moneyline.away < moneyline.home ? 'text-success' : 'text-base-content'}`}>
                  {americanOdds(moneyline.away)}
                </span>
                <span className={`text-sm font-mono font-bold ${moneyline.home < moneyline.away ? 'text-success' : 'text-base-content'}`}>
                  {americanOdds(moneyline.home)}
                </span>
              </>
            ) : (
              <span className="text-xs text-base-content/30">—</span>
            )}
          </div>

          {/* Spread */}
          <div className="flex flex-col items-center bg-base-300 rounded-lg p-2">
            <span className="text-[10px] uppercase tracking-wider text-base-content/40 mb-1">Spread</span>
            {spread ? (
              <>
                <span className="text-sm font-mono font-bold">
                  {spread.line > 0 ? '+' : ''}{-spread.line} <span className="text-base-content/50 text-xs">{americanOdds(spread.away)}</span>
                </span>
                <span className="text-sm font-mono font-bold">
                  {spread.line > 0 ? '' : '+'}{spread.line} <span className="text-base-content/50 text-xs">{americanOdds(spread.home)}</span>
                </span>
              </>
            ) : (
              <span className="text-xs text-base-content/30">—</span>
            )}
          </div>

          {/* Total */}
          <div className="flex flex-col items-center bg-base-300 rounded-lg p-2">
            <span className="text-[10px] uppercase tracking-wider text-base-content/40 mb-1">O/U</span>
            {total ? (
              <>
                <span className="text-sm font-mono font-bold">
                  O {total.line} <span className="text-base-content/50 text-xs">{americanOdds(total.over)}</span>
                </span>
                <span className="text-sm font-mono font-bold">
                  U {total.line} <span className="text-base-content/50 text-xs">{americanOdds(total.under)}</span>
                </span>
              </>
            ) : (
              <span className="text-xs text-base-content/30">—</span>
            )}
          </div>
        </div>

        {moneyline && (
          <span className="text-[10px] text-base-content/30 text-right">via {moneyline.book}</span>
        )}

        {/* Expand for props */}
        <button
          className="btn btn-ghost btn-xs gap-1"
          onClick={handleExpand}
        >
          <TrendingUp size={12} />
          Player Props
          {expanded ? <ChevronUp size={12} /> : <ChevronDown size={12} />}
        </button>

        {expanded && (
          <div className="flex flex-col gap-2 pt-1">
            {loadingProps && (
              <div className="flex justify-center py-4">
                <span className="loading loading-spinner loading-sm text-primary" />
              </div>
            )}
            {!loadingProps && props.length === 0 && (
              <div className="text-center py-4">
                <p className="text-base-content/40 text-xs">Props not yet posted</p>
                <p className="text-base-content/30 text-[10px] mt-1">
                  Typically available 12–24hrs before game time
                </p>
              </div>
            )}
            {props.map((p, i) => (
              <PropCard key={`${p.player}-${p.market}-${i}`} prop={p} />
            ))}
          </div>
        )}
      </div>
    </div>
  );
};
