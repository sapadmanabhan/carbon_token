import { useState, useEffect, useCallback } from 'react'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell,
         LineChart, Line } from 'recharts'

const API = import.meta.env.VITE_API_URL || 'http://localhost:5050'
const US_STATES = ['ca', 'tx', 'wa', 'wv', 'ny', 'pa', 'fl', 'oh', 'il', 'ga']

export default function App() {
  const [token, setToken] = useState(localStorage.getItem('token') || '')
  const [me, setMe] = useState(null)
  const [cleanest, setCleanest] = useState([])
  const [stateBudget, setStateBudget] = useState(null)
  const [lastRequest, setLastRequest] = useState(null)
  const [alerts, setAlerts] = useState([])
  const [balanceHistory, setBalanceHistory] = useState([])

  const refresh = useCallback(async () => {
    try {
      const c = await fetch(`${API}/api/carbon/cleanest`)
      if (c.ok) setCleanest((await c.json()).regions)
    } catch (e) {}

    if (!token) { setMe(null); return }
    try {
      const r = await fetch(`${API}/me`, { headers: { Authorization: `Bearer ${token}` } })
      if (r.ok) {
        const data = await r.json()
        setMe(data)
        checkAlerts(data)
        setBalanceHistory(prev => [...prev, { t: Date.now(), balance: data.balance }].slice(-60))
        if (data.state) {
          try {
            const b = await fetch(`${API}/api/state/${data.state}/budget`)
            if (b.ok) setStateBudget(await b.json())
          } catch (e) {}
        }
      } else if (r.status === 401) {
        setToken(''); localStorage.removeItem('token'); setMe(null)
      }
    } catch (e) {}
  }, [token])

  useEffect(() => {
    refresh()
    const id = setInterval(refresh, 5000)
    return () => clearInterval(id)
  }, [refresh])

  function checkAlerts(data) {
    if (!data.granted_today) return setAlerts([])
    const pctUsed = 1 - data.balance / data.granted_today
    const active = []
    if (pctUsed >= 0.95) active.push({ level: 'critical', msg: '🚨 95% USED — next request may fail' })
    else if (pctUsed >= 0.80) active.push({ level: 'critical', msg: '🔴 80% used — slow down' })
    else if (pctUsed >= 0.50) active.push({ level: 'warning', msg: '⚠️ 50% used — halfway through the day' })
    setAlerts(active)
  }

  if (!token) return <AuthView onToken={(t) => { setToken(t); localStorage.setItem('token', t) }} />

  return (
    <div style={{ maxWidth: 1200, margin: '0 auto', padding: 24 }}>
      <Header me={me} onLogout={() => { setToken(''); localStorage.removeItem('token') }} />
      {alerts.map((a, i) => <Alert key={i} level={a.level} msg={a.msg} />)}
      <TopRow me={me} balanceHistory={balanceHistory} />
      <MiddleRow me={me} stateBudget={stateBudget} />
      <SpendButtons token={token} onResult={(r) => { setLastRequest(r); refresh() }} />
      {lastRequest && <LastRequest result={lastRequest} />}
      <Cleanest regions={cleanest} myState={me?.state} />
    </div>
  )
}

function AuthView({ onToken }) {
  const [mode, setMode] = useState('login')
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [state, setState] = useState('ca')
  const [error, setError] = useState('')

  async function submit() {
    setError('')
    try {
      if (mode === 'register') {
        const r = await fetch(`${API}/register`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ username, password, state }),
        })
        if (!r.ok) { setError((await r.json()).error || 'registration failed'); return }
      }
      const r = await fetch(`${API}/login`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password }),
      })
      if (!r.ok) { setError((await r.json()).error || 'login failed'); return }
      onToken((await r.json()).token)
    } catch (e) {
      setError('network error — is backend running on ' + API + '?')
    }
  }

  return (
    <div style={{ maxWidth: 420, margin: '80px auto', padding: 32, background: '#1e293b', borderRadius: 12 }}>
      <h1 style={{ marginTop: 0, color: '#10b981' }}>🌱 Carbon Trust Engine</h1>
      <p style={{ color: '#94a3b8', marginTop: -8 }}>Pollution-budgeted AI gateway</p>
      <div style={{ display: 'flex', gap: 8, marginBottom: 20 }}>
        <button onClick={() => setMode('login')} style={tabStyle(mode === 'login')}>Login</button>
        <button onClick={() => setMode('register')} style={tabStyle(mode === 'register')}>Register</button>
      </div>
      <label style={labelStyle}>Username</label>
      <input value={username} onChange={e => setUsername(e.target.value)} style={inputStyle} />
      <label style={labelStyle}>Password</label>
      <input type="password" value={password} onChange={e => setPassword(e.target.value)} style={inputStyle} />
      {mode === 'register' && (
        <>
          <label style={labelStyle}>State</label>
          <select value={state} onChange={e => setState(e.target.value)} style={inputStyle}>
            {US_STATES.map(s => <option key={s} value={s}>{s.toUpperCase()}</option>)}
          </select>
        </>
      )}
      <button onClick={submit} style={primaryButtonStyle}>
        {mode === 'login' ? 'Log in' : 'Register & log in'}
      </button>
      {error && <div style={{ color: '#f87171', marginTop: 12 }}>{error}</div>}
    </div>
  )
}

function Header({ me, onLogout }) {
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 20 }}>
      <div>
        <h1 style={{ margin: 0, color: '#10b981' }}>🌱 Carbon Trust Engine</h1>
        <div style={{ color: '#94a3b8', fontSize: 14 }}>User #{me?.user_id} · State {me?.state?.toUpperCase()}</div>
      </div>
      <button onClick={onLogout} style={secondaryButtonStyle}>Log out</button>
    </div>
  )
}

function Alert({ level, msg }) {
  const colors = { critical: { bg: '#7f1d1d', border: '#dc2626' }, warning: { bg: '#78350f', border: '#f59e0b' } }
  const c = colors[level]
  return (
    <div style={{ background: c.bg, borderLeft: `4px solid ${c.border}`, padding: '12px 16px', borderRadius: 6, marginBottom: 12, fontWeight: 600 }}>
      {msg}
    </div>
  )
}

function TopRow({ me, balanceHistory }) {
  if (!me) return <div style={{ padding: 24 }}>Loading…</div>
  return (
    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 16, marginBottom: 16 }}>
      <BalanceRing me={me} />
      <VelocityCard history={balanceHistory} />
      <CountdownCard />
    </div>
  )
}

function BalanceRing({ me }) {
  const pctLeft = me.granted_today ? (me.balance / me.granted_today) * 100 : 0
  const pctUsed = 100 - pctLeft
  const color = pctLeft > 50 ? '#10b981' : pctLeft > 20 ? '#f59e0b' : '#dc2626'
  const radius = 60
  const circumference = 2 * Math.PI * radius
  const offset = circumference * (1 - pctLeft / 100)
  const badges = [
    { label: '50%', hit: pctUsed >= 50 },
    { label: '80%', hit: pctUsed >= 80 },
    { label: '95%', hit: pctUsed >= 95 },
  ]
  return (
    <Card title="Balance">
      <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
        <svg width="150" height="150" style={{ flexShrink: 0 }}>
          <circle cx="75" cy="75" r={radius} stroke="#334155" strokeWidth="12" fill="none" />
          <circle cx="75" cy="75" r={radius} stroke={color} strokeWidth="12" fill="none"
                  strokeDasharray={circumference} strokeDashoffset={offset}
                  transform="rotate(-90 75 75)" style={{ transition: 'stroke-dashoffset 0.5s' }} />
          <text x="75" y="70" textAnchor="middle" fill="#e2e8f0" fontSize="28" fontWeight="700">{me.balance}</text>
          <text x="75" y="90" textAnchor="middle" fill="#94a3b8" fontSize="12">/{me.granted_today}</text>
        </svg>
        <div style={{ flex: 1 }}>
          <div style={{ fontSize: 11, color: '#94a3b8', marginBottom: 8 }}>THRESHOLDS HIT</div>
          {badges.map((b, i) => (
            <div key={i} style={{
              display: 'inline-block', marginRight: 6, marginBottom: 6,
              padding: '3px 10px', borderRadius: 12, fontSize: 12, fontWeight: 600,
              background: b.hit ? (b.label === '95%' || b.label === '80%' ? '#dc2626' : '#f59e0b') : '#334155',
              color: b.hit ? '#fff' : '#64748b',
            }}>{b.label} {b.hit ? '✓' : ''}</div>
          ))}
          <div style={{ color: '#94a3b8', fontSize: 13, marginTop: 8 }}>{pctUsed.toFixed(0)}% used today</div>
        </div>
      </div>
    </Card>
  )
}

function VelocityCard({ history }) {
  const now = Date.now()
  const recent = history.filter(h => now - h.t < 120000)
  let velocity = 0
  if (recent.length >= 2) {
    const first = recent[0]
    const last = recent[recent.length - 1]
    const mins = (last.t - first.t) / 60000
    if (mins > 0) velocity = Math.max(0, (first.balance - last.balance) / mins)
  }
  return (
    <Card title="Spend velocity">
      <div style={{ fontSize: 40, fontWeight: 700, color: '#60a5fa' }}>{velocity.toFixed(1)}</div>
      <div style={{ color: '#94a3b8', fontSize: 13 }}>tokens / minute (last 2 min)</div>
      {history.length > 3 && (
        <div style={{ height: 60, marginTop: 8 }}>
          <ResponsiveContainer>
            <LineChart data={history}>
              <Line type="monotone" dataKey="balance" stroke="#60a5fa" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}
    </Card>
  )
}

function CountdownCard() {
  const [now, setNow] = useState(new Date())
  useEffect(() => {
    const id = setInterval(() => setNow(new Date()), 1000)
    return () => clearInterval(id)
  }, [])
  const tomorrow = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1))
  const diff = tomorrow - now
  const h = Math.floor(diff / 3600000)
  const m = Math.floor((diff % 3600000) / 60000)
  const s = Math.floor((diff % 60000) / 1000)
  return (
    <Card title="Daily reset (00:00 UTC)">
      <div style={{ fontSize: 36, fontWeight: 700, color: '#a78bfa', fontVariantNumeric: 'tabular-nums' }}>
        {String(h).padStart(2, '0')}:{String(m).padStart(2, '0')}:{String(s).padStart(2, '0')}
      </div>
      <div style={{ color: '#94a3b8', fontSize: 13 }}>until Oracle + Allocator run</div>
    </Card>
  )
}

function MiddleRow({ me, stateBudget }) {
  return (
    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 16 }}>
      <IntensityCard me={me} />
      <StateBudgetCard budget={stateBudget} />
    </div>
  )
}

function IntensityCard({ me }) {
  if (!me) return null
  const intensity = me.current_intensity
  const color = intensity < 200 ? '#22c55e' : intensity < 400 ? '#eab308' : '#dc2626'
  return (
    <Card title="Your grid right now">
      <div style={{ display: 'flex', alignItems: 'baseline', gap: 12 }}>
        <div style={{ fontSize: 42, fontWeight: 700, color }}>{intensity?.toFixed(0)}</div>
        <div style={{ color: '#94a3b8' }}>gCO₂/kWh</div>
      </div>
      <div style={{ color: '#64748b', fontSize: 12 }}>source: {me.intensity_source}</div>
      <div style={{ marginTop: 12, fontSize: 14, color: '#cbd5e1' }}>
        Inference costs <b style={{ color: '#10b981' }}>{me.cost_now?.inference}</b> tokens<br />
        Training costs <b style={{ color: '#8b5cf6' }}>{me.cost_now?.training}</b> tokens
      </div>
    </Card>
  )
}

function StateBudgetCard({ budget }) {
  if (!budget) return <Card title="State pool"><div style={{ color: '#64748b' }}>Loading…</div></Card>
  const b = budget.budget
  return (
    <Card title={`${budget.state?.toUpperCase()} state pool`}>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
        <div>
          <div style={{ fontSize: 11, color: '#94a3b8' }}>DAILY CAP</div>
          <div style={{ fontSize: 24, fontWeight: 700 }}>{b?.cap_tokens ?? '—'}</div>
        </div>
        <div>
          <div style={{ fontSize: 11, color: '#94a3b8' }}>RESERVE LEFT</div>
          <div style={{ fontSize: 24, fontWeight: 700, color: '#60a5fa' }}>{budget.reserve_tokens}</div>
        </div>
        <div>
          <div style={{ fontSize: 11, color: '#94a3b8' }}>USERS IN STATE</div>
          <div style={{ fontSize: 24, fontWeight: 700, color: '#a78bfa' }}>👤 {budget.registered_users}</div>
        </div>
        <div>
          <div style={{ fontSize: 11, color: '#94a3b8' }}>24H AVG</div>
          <div style={{ fontSize: 24, fontWeight: 700 }}>{b?.intensity_avg?.toFixed(0) ?? '—'}</div>
        </div>
      </div>
    </Card>
  )
}

function SpendButtons({ token, onResult }) {
  const [loading, setLoading] = useState(null)
  async function spend(route) {
    setLoading(route)
    try {
      const r = await fetch(`${API}${route}`, { method: 'POST', headers: { Authorization: `Bearer ${token}` } })
      const body = await r.json()
      onResult({ route, status: r.status, body })
    } catch (e) {
      onResult({ route, status: 0, body: { error: e.message } })
    } finally { setLoading(null) }
  }
  return (
    <Card title="Send a request">
      <div style={{ display: 'flex', gap: 12 }}>
        <button onClick={() => spend('/api/v1/inference')} disabled={loading}
                style={{ ...primaryButtonStyle, width: 'auto', flex: 1 }}>
          {loading === '/api/v1/inference' ? '…' : '🔍 Inference'}
        </button>
        <button onClick={() => spend('/api/v1/training')} disabled={loading}
                style={{ ...primaryButtonStyle, width: 'auto', flex: 1, background: '#8b5cf6' }}>
          {loading === '/api/v1/training' ? '…' : '🧠 Training'}
        </button>
      </div>
    </Card>
  )
}

function LastRequest({ result }) {
  const ok = result.status === 200
  const blocked = result.status === 429
  return (
    <Card title="Last request">
      <div style={{ fontSize: 18, fontWeight: 600, color: ok ? '#10b981' : blocked ? '#f59e0b' : '#dc2626' }}>
        {ok ? '✓ ALLOWED' : blocked ? '⛔ BLOCKED' : '✗ ERROR'}
        {result.body?.cost !== undefined && <span style={{ marginLeft: 12 }}>cost: {result.body.cost}</span>}
      </div>
      {result.body?.cost_breakdown && (
        <div style={{ fontSize: 14, color: '#94a3b8', marginTop: 8 }}>
          base {result.body.cost_breakdown.base} × {result.body.cost_breakdown.multiplier}
          {' @ '}{result.body.cost_breakdown.intensity?.toFixed(0)} gCO₂/kWh
        </div>
      )}
      {blocked && result.body?.suggestions?.cleaner_states_now && (
        <div style={{ marginTop: 12 }}>
          <div style={{ color: '#94a3b8', fontSize: 14, marginBottom: 6 }}>Migrate to:</div>
          {result.body.suggestions.cleaner_states_now.map(s => (
            <span key={s.state} style={{ display: 'inline-block', background: '#10b981', color: '#022c22', padding: '4px 10px', borderRadius: 12, marginRight: 6, fontWeight: 600 }}>
              {s.state.toUpperCase()}: {s.intensity.toFixed(0)}
            </span>
          ))}
        </div>
      )}
    </Card>
  )
}

function Cleanest({ regions, myState }) {
  if (!regions.length) return null
  const data = regions.map(r => ({ state: r.state.toUpperCase(), intensity: Math.round(r.intensity), isMe: r.state === myState }))
  return (
    <Card title="US grid, cleanest → dirtiest (gCO₂/kWh)">
      <div style={{ height: 260 }}>
        <ResponsiveContainer>
          <BarChart data={data} margin={{ top: 10, right: 10, bottom: 10, left: 10 }}>
            <XAxis dataKey="state" stroke="#94a3b8" />
            <YAxis stroke="#94a3b8" />
            <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155' }} />
            <Bar dataKey="intensity">
              {data.map((d, i) => (
                <Cell key={i} fill={d.isMe ? '#10b981' : d.intensity < 200 ? '#22c55e' : d.intensity < 400 ? '#eab308' : '#dc2626'} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </Card>
  )
}

function Card({ title, children }) {
  return (
    <div style={{ background: '#1e293b', borderRadius: 12, padding: 20 }}>
      <div style={{ color: '#94a3b8', fontSize: 11, textTransform: 'uppercase', letterSpacing: 1, marginBottom: 12 }}>{title}</div>
      {children}
    </div>
  )
}

const inputStyle = { width: '100%', padding: '10px 12px', marginBottom: 12, background: '#0f172a', border: '1px solid #334155', borderRadius: 6, color: '#e2e8f0', fontSize: 14, boxSizing: 'border-box' }
const labelStyle = { display: 'block', fontSize: 13, color: '#94a3b8', marginBottom: 4 }
const primaryButtonStyle = { width: '100%', padding: '12px 16px', background: '#10b981', color: '#022c22', border: 'none', borderRadius: 6, fontSize: 15, fontWeight: 600, cursor: 'pointer' }
const secondaryButtonStyle = { padding: '8px 14px', background: 'transparent', color: '#94a3b8', border: '1px solid #334155', borderRadius: 6, cursor: 'pointer' }
const tabStyle = (active) => ({ flex: 1, padding: '8px 12px', background: active ? '#10b981' : 'transparent', color: active ? '#022c22' : '#94a3b8', border: `1px solid ${active ? '#10b981' : '#334155'}`, borderRadius: 6, cursor: 'pointer', fontWeight: 600 })
