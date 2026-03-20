'use client'
import { useState } from 'react'
import { TrendingUp, Zap, Shield, Activity, ChevronUp, ChevronDown, RefreshCw } from 'lucide-react'

interface PropCard {
  id: string
  player: string
  team: string
  opponent: string
  propType: string
  line: number
  overOdds: number
  underOdds: number
  modelOver: number
  vegasOver: number
  edgePct: number
  isPlayable: boolean
  recommendation: 'OVER' | 'UNDER'
  confidence: 'HIGH' | 'MEDIUM' | 'LOW'
}

const MOCK_PROPS: PropCard[] = [
  { id: '1', player: 'Aaron Judge', team: 'NYY', opponent: 'BOS', propType: 'Total Bases', line: 1.5, overOdds: -130, underOdds: 108, modelOver: 68.4, vegasOver: 56.5, edgePct: 11.9, isPlayable: true, recommendation: 'OVER', confidence: 'HIGH' },
  { id: '2', player: 'Gerrit Cole', team: 'NYY', opponent: 'BOS', propType: 'Strikeouts', line: 7.5, overOdds: -115, underOdds: -105, modelOver: 61.2, vegasOver: 54.8, edgePct: 6.4, isPlayable: true, recommendation: 'OVER', confidence: 'HIGH' },
  { id: '3', player: 'Mookie Betts', team: 'LAD', opponent: 'SF', propType: 'Hits+Runs+RBIs', line: 2.5, overOdds: 120, underOdds: -145, modelOver: 55.1, vegasOver: 45.5, edgePct: 9.6, isPlayable: true, recommendation: 'OVER', confidence: 'MEDIUM' },
  { id: '4', player: 'Shohei Ohtani', team: 'LAD', opponent: 'SF', propType: 'Home Runs', line: 0.5, overOdds: 210, underOdds: -260, modelOver: 32.1, vegasOver: 27.8, edgePct: 4.3, isPlayable: true, recommendation: 'OVER', confidence: 'MEDIUM' },
  { id: '5', player: 'Spencer Strider', team: 'ATL', opponent: 'NYM', propType: 'Strikeouts', line: 9.5, overOdds: -105, underOdds: -115, modelOver: 48.2, vegasOver: 52.4, edgePct: -4.2, isPlayable: false, recommendation: 'UNDER', confidence: 'LOW' },
]

const confidenceColor = { HIGH: '#10b981', MEDIUM: '#f59e0b', LOW: '#6b7280' }
const confidenceBg = { HIGH: 'bg-emerald-500/20 text-emerald-400', MEDIUM: 'bg-amber-500/20 text-amber-400', LOW: 'bg-zinc-700 text-zinc-400' }

function PropCardComponent({ prop }: { prop: PropCard }) {
  const isOver = prop.recommendation === 'OVER'
  const confColor = confidenceColor[prop.confidence]
  const barWidth = Math.min(Math.abs(prop.edgePct) * 5, 100)

  return (
    <div className={`rounded-xl border p-5 transition-all duration-200 hover:scale-[1.01] ${prop.isPlayable ? 'border-emerald-500/30 bg-zinc-900/80 shadow-lg shadow-emerald-500/5' : 'border-zinc-800 bg-zinc-900/40 opacity-70'}`}>
      <div className="flex items-start justify-between mb-4">
        <div>
          <p className="text-xs font-semibold text-zinc-400 uppercase tracking-widest mb-1">{prop.propType}</p>
          <h3 className="text-white font-bold text-lg leading-tight">{prop.player}</h3>
          <p className="text-zinc-500 text-sm">{prop.team} vs {prop.opponent}</p>
        </div>
        <div className="flex flex-col items-end gap-2">
          <span className={`text-xs font-bold px-2.5 py-1 rounded-full ${confidenceBg[prop.confidence]}`}>
            {prop.confidence}
          </span>
          {prop.isPlayable && (
            <span className="text-xs font-bold px-2.5 py-1 rounded-full bg-emerald-500/20 text-emerald-400 flex items-center gap-1">
              <Zap size={10} /> +EV
            </span>
          )}
        </div>
      </div>

      <div className="flex items-center justify-between mb-4">
        <div className="text-center">
          <p className="text-zinc-500 text-xs mb-1">Line</p>
          <p className="text-white font-bold text-xl">{prop.line}</p>
        </div>
        <div className={`flex items-center gap-1 px-4 py-2 rounded-lg font-bold text-lg ${isOver ? 'bg-emerald-500/20 text-emerald-400' : 'bg-red-500/20 text-red-400'}`}>
          {isOver ? <ChevronUp size={20} /> : <ChevronDown size={20} />}
          {prop.recommendation}
          <span className="text-sm ml-1">{isOver ? prop.overOdds > 0 ? '+' + prop.overOdds : prop.overOdds : prop.underOdds > 0 ? '+' + prop.underOdds : prop.underOdds}</span>
        </div>
        <div className="text-center">
          <p className="text-zinc-500 text-xs mb-1">Edge</p>
          <p className={`font-bold text-xl ${prop.edgePct > 0 ? 'text-emerald-400' : 'text-red-400'}`}>
            {prop.edgePct > 0 ? '+' : ''}{prop.edgePct}%
          </p>
        </div>
      </div>

      <div className="space-y-2">
        <div className="flex justify-between text-xs text-zinc-500 mb-1">
          <span>Model: <span className="text-white font-semibold">{prop.modelOver}%</span></span>
          <span>Vegas: <span className="text-zinc-300">{prop.vegasOver}%</span></span>
        </div>
        <div className="h-1.5 bg-zinc-800 rounded-full overflow-hidden">
          <div className="h-full rounded-full transition-all duration-500" style={{ width: `${prop.modelOver}%`, background: `linear-gradient(90deg, ${confColor}, ${confColor}88)` }} />
        </div>
      </div>
    </div>
  )
}

export default function Home() {
  const [filter, setFilter] = useState<'all' | 'playable'>('playable')
  const [propType, setPropType] = useState<string>('all')
  const [refreshing, setRefreshing] = useState(false)

  const propTypes = ['all', ...Array.from(new Set(MOCK_PROPS.map(p => p.propType)))]
  const filtered = MOCK_PROPS.filter(p => {
    if (filter === 'playable' && !p.isPlayable) return false
    if (propType !== 'all' && p.propType !== propType) return false
    return true
  }).sort((a, b) => b.edgePct - a.edgePct)

  const playableCount = MOCK_PROPS.filter(p => p.isPlayable).length
  const avgEdge = (MOCK_PROPS.filter(p => p.isPlayable).reduce((s, p) => s + p.edgePct, 0) / playableCount).toFixed(1)

  const handleRefresh = () => {
    setRefreshing(true)
    setTimeout(() => setRefreshing(false), 1000)
  }

  return (
    <main className="min-h-screen bg-zinc-950 text-white">
      {/* Header */}
      <header className="border-b border-zinc-800 bg-zinc-950/90 backdrop-blur-md sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 h-16 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-emerald-400 to-cyan-500 flex items-center justify-center">
              <TrendingUp size={16} className="text-black" />
            </div>
            <span className="font-bold text-lg tracking-tight">PropIQ</span>
            <span className="text-xs text-zinc-500 border border-zinc-700 rounded px-1.5 py-0.5">v2.0</span>
          </div>
          <div className="flex items-center gap-4">
            <div className="flex items-center gap-1.5 text-xs text-emerald-400">
              <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" />
              Live
            </div>
            <button onClick={handleRefresh} className="p-2 rounded-lg bg-zinc-800 hover:bg-zinc-700 transition-colors">
              <RefreshCw size={14} className={refreshing ? 'animate-spin text-emerald-400' : 'text-zinc-400'} />
            </button>
          </div>
        </div>
      </header>

      <div className="max-w-7xl mx-auto px-4 sm:px-6 py-8">
        {/* Stats Bar */}
        <div className="grid grid-cols-3 gap-4 mb-8">
          {[
            { icon: <Zap size={16} />, label: 'Playable Props', value: playableCount, color: 'text-emerald-400' },
            { icon: <Activity size={16} />, label: 'Avg Edge', value: `+${avgEdge}%`, color: 'text-cyan-400' },
            { icon: <Shield size={16} />, label: 'Model Accuracy', value: '67.6%', color: 'text-violet-400' },
          ].map(stat => (
            <div key={stat.label} className="bg-zinc-900/60 border border-zinc-800 rounded-xl p-4 flex items-center gap-3">
              <div className={`${stat.color} opacity-80`}>{stat.icon}</div>
              <div>
                <p className="text-zinc-500 text-xs">{stat.label}</p>
                <p className={`font-bold text-lg ${stat.color}`}>{stat.value}</p>
              </div>
            </div>
          ))}
        </div>

        {/* Filters */}
        <div className="flex flex-wrap items-center gap-3 mb-6">
          <div className="flex rounded-lg border border-zinc-700 overflow-hidden">
            {(['all', 'playable'] as const).map(f => (
              <button key={f} onClick={() => setFilter(f)}
                className={`px-4 py-2 text-sm font-medium transition-colors ${filter === f ? 'bg-emerald-500/20 text-emerald-400' : 'text-zinc-400 hover:text-white'}`}>
                {f === 'all' ? 'All Props' : '⚡ Playable Only'}
              </button>
            ))}
          </div>
          <div className="flex gap-2 flex-wrap">
            {propTypes.map(pt => (
              <button key={pt} onClick={() => setPropType(pt)}
                className={`px-3 py-1.5 text-xs font-medium rounded-lg border transition-colors ${propType === pt ? 'border-emerald-500/50 bg-emerald-500/10 text-emerald-400' : 'border-zinc-700 text-zinc-400 hover:border-zinc-500'}`}>
                {pt === 'all' ? 'All Types' : pt}
              </button>
            ))}
          </div>
        </div>

        {/* Props Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {filtered.map(prop => <PropCardComponent key={prop.id} prop={prop} />)}
        </div>

        {filtered.length === 0 && (
          <div className="text-center py-16 text-zinc-600">
            <Zap size={32} className="mx-auto mb-3 opacity-30" />
            <p>No props match your filters right now.</p>
          </div>
        )}
      </div>
    </main>
  )
}
