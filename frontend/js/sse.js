// Single EventSource per station. Multiple views (grid cell + detail tab)
// share the same connection via a subscriber list.

const streams = new Map(); // station -> { es, subs:Set<fn> }

export function subscribe(station, onEvent) {
  let entry = streams.get(station);
  if (!entry) {
    const es = new EventSource(`/api/stream/${encodeURIComponent(station)}`);
    entry = { es, subs: new Set() };
    streams.set(station, entry);
    const dispatch = (type, ev) => {
      try {
        const data = JSON.parse(ev.data);
        entry.subs.forEach(fn => { try { fn(data); } catch (e) { console.warn(e); } });
      } catch (_) { /* ignore */ }
    };
    es.addEventListener('candle_update', (ev) => dispatch('candle_update', ev));
    es.addEventListener('candle_frozen', (ev) => dispatch('candle_frozen', ev));
    es.addEventListener('station_stalled', (ev) => dispatch('station_stalled', ev));
    es.onerror = () => { /* EventSource auto-reconnects */ };
  }
  entry.subs.add(onEvent);
  return () => {
    entry.subs.delete(onEvent);
    if (entry.subs.size === 0) {
      entry.es.close();
      streams.delete(station);
    }
  };
}
