import React from 'react';
import { RefreshCw, Zap, BarChart3 } from 'lucide-react';
import { ViewMode } from '../types';

interface HeaderProps {
  view: ViewMode;
  onViewChange: (v: ViewMode) => void;
  onRefresh: () => void;
  loading: boolean;
  eventCount: number;
  propCount: number;
}

export const Header: React.FC<HeaderProps> = ({ view, onViewChange, onRefresh, loading, eventCount, propCount }) => {
  return (
    <div className="flex flex-col gap-3 pb-4 border-b border-base-300">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Zap className="text-primary" size={22} />
          <span className="text-lg font-bold tracking-tight">PropIQ</span>
          <span className="badge badge-primary badge-sm">LIVE</span>
        </div>
        <button
          className={`btn btn-ghost btn-sm ${loading ? 'animate-spin' : ''}`}
          onClick={onRefresh}
          disabled={loading}
        >
          <RefreshCw size={16} />
        </button>
      </div>

      <div className="flex gap-2">
        <button
          className={`btn btn-sm ${view === 'games' ? 'btn-primary' : 'btn-ghost'}`}
          onClick={() => onViewChange('games')}
        >
          <BarChart3 size={14} />
          Games ({eventCount})
        </button>
        <button
          className={`btn btn-sm ${view === 'props' ? 'btn-secondary' : 'btn-ghost'}`}
          onClick={() => onViewChange('props')}
        >
          <Zap size={14} />
          Player Props ({propCount})
        </button>
      </div>
    </div>
  );
};
