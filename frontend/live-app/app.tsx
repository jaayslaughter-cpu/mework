import React, { useState, useEffect } from 'react';
import { createRoot } from 'react-dom/client';
import { AlertTriangle } from 'lucide-react';
import { GameOdds, PlayerProp, ViewMode } from './types';
import { fetchEvents, fetchGameOdds, fetchPlayerProps } from './utils/api';
import { Header } from './components/Header';
import { GameCard } from './components/GameCard';
import { PropCard } from './components/PropCard';

const App: React.FC = () => {
  const [games, setGames] = useState<GameOdds[]>([]);
  const [allProps, setAllProps] = useState<PlayerProp[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [view, setView] = useState<ViewMode>('games');

  const loadData = async () => {
    setLoading(true);
    setError(null);
    try {
      const loaders: Record<ViewMode, () => Promise<any[]>> = {
        games: async () => {
          const events = await fetchEvents();
          if (!Array.isArray(events)) throw new Error('Unexpected API response');
          const results = await Promise.allSettled(
            events.map(e => fetchGameOdds(e))
          );
          const loaded: GameOdds[] = results
            .filter((r): r is PromiseFulfilledResult<GameOdds> => r.status === 'fulfilled')
            .map(r => r.value)
            .sort(
              (a, b) =>
                new Date(a.event.commence_time).getTime() -
                new Date(b.event.commence_time).getTime()
            );
          return loaded;
        },
        props: async () => {
          const props = await fetchPlayerProps();
          if (!Array.isArray(props)) throw new Error('Unexpected API response');
          return props;
        }
      };
      const data = await loaders[view]();
      const setters: Record<ViewMode, (data: any[]) => void> = {
        games: setGames,
        props: setAllProps
      };
      setters[view](data);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
  const loadData = async () => {
    try {
      // ...previous data fetching logic...
      setLoading(false);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadData();
  }, [view]);

  // ...rest of component rendering logic
};

export default App;

const loadData = async () => {
  try {
    setGames(loadedGames);

    // Try to fetch props for first 3 events
    const propResults = await Promise.allSettled(
      events.slice(0, 3).map(e => fetchPlayerProps(e.id))
    );
    const gathered: PlayerProp[] = [];
    for (const r of propResults) {
      if (r.status === 'fulfilled') gathered.push(...r.value);
    }
    setAllProps(gathered);
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : 'Unknown error';
    console.error('Failed to load data:', e);
    setError(msg);
  } finally {
    setLoading(false);
  }
};

useEffect(() => {
  loadData();
}, []);

return (
  <div className="min-h-screen bg-base-100 text-base-content p-4 flex flex-col gap-4 max-w-lg mx-auto">
    <Header
      view={view}
      onViewChange={setView}
      onRefresh={loadData}
      loading={loading}
      eventCount={games.length}
      propCount={allProps.length}
    />

    {error && (
      <div className="alert alert-error">
          <AlertTriangle size={16} />
          <span className="text-sm">{error}</span>
        </div>
      )}

      {loading && games.length === 0 && (
        <div className="flex flex-col items-center justify-center py-16 gap-3">
          <span className="loading loading-spinner loading-lg text-primary" />
          <span className="text-base-content/50 text-sm">Fetching live MLB odds...</span>
        </div>
      )}

      {view === 'games' && (
        <div className="flex flex-col gap-3">
          {games.map(g => (
            <GameCard key={g.event.id} game={g} />
          ))}
          {!loading && games.length === 0 && !error && (
            <div className="text-center py-12 text-base-content/40">
              No MLB events scheduled right now
            </div>
          )}
        </div>
      )}

      {view === 'props' && (
        <div className="flex flex-col gap-3">
          {allProps.length === 0 && !loading && (
            <div className="card bg-base-200 p-6 text-center">
              <p className="text-base-content/50 text-sm mb-2">No player props available yet</p>
              <p className="text-base-content/30 text-xs">
                Player props are typically posted 12–24 hours before game time.
                Opening Day is March 26 — check back soon!
              </p>
              <p className="text-base-content/30 text-xs mt-2">
                Expand any game card in the Games tab to check for props on individual matchups.
              </p>
            </div>
          )}
          {allProps.map((p, i) => (
            <PropCard key={`${p.player}-${p.market}-${i}`} prop={p} />
          ))}
        </div>
      )}

      <div className="text-center text-[10px] text-base-content/20 pb-2">
        PropIQ Analytics • Powered by The Odds API • Data refreshed on demand
      </div>
    </div>
  );
};

const rootElement = document.getElementById('root');
if (!rootElement) {
  throw new Error('Failed to find root element');
}
createRoot(rootElement).render(<App />);
