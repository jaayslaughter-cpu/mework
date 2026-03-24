import React from 'react';
import { TrendingUp, TrendingDown } from 'lucide-react';
import { PlayerProp } from '../types';
import { americanOdds } from '../utils/api';

interface PropCardProps {
  prop: PlayerProp;
}

export const PropCard: React.FC<PropCardProps> = ({ prop }) => {
  const isOverFavorite = prop.overPrice < prop.underPrice;

  return (
    <div className="bg-base-300 rounded-lg p-3">
      <div className="flex items-center justify-between mb-2">
        <span className="font-semibold text-sm">{prop.player}</span>
        <span className="badge badge-sm">{prop.marketLabel}</span>
      </div>
      <div className="flex items-center gap-2">
        <span className="text-base-content/50 text-xs font-mono">Line: {prop.line}</span>
      </div>
      <div className="grid grid-cols-2 gap-2 mt-2">
        <button className={`btn btn-sm ${isOverFavorite ? 'btn-success btn-outline' : 'btn-ghost'} gap-1`}>
          <TrendingUp size={12} />
          Over {americanOdds(prop.overPrice)}
        </button>
        <button className={`btn btn-sm ${!isOverFavorite ? 'btn-error btn-outline' : 'btn-ghost'} gap-1`}>
          <TrendingDown size={12} />
          Under {americanOdds(prop.underPrice)}
        </button>
      </div>
      <span className="text-[10px] text-base-content/30 mt-1 block">via {prop.bookmaker}</span>
    </div>
  );
};
