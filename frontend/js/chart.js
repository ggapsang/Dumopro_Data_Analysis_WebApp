// Pure Canvas boxplot renderer.

const CANDLE_WIDTH_RATIO = 0.78;   // body/slot 비율 — 간격 좁게
const CANDLE_WIDTH_MAX = 30;       // body 최대 픽셀 폭
const CANDLE_WIDTH_MIN = 4;
const X_LABEL_GAP_PX = 6;          // 라벨 사이 최소 여백

const COLORS = {
  body: '#6b96c8',
  bodyStroke: '#000000',
  median: '#000000',
  whisker: '#000000',
  outlier: '#c89040',
  extreme: '#a00000',
  ma: '#1a7a3a',
  liveBody: '#c6c0a0',
  axis: '#000000',
  grid: '#dddddd',
  trend: '#7030a0',
  band: 'rgba(112,48,160,0.15)',
  residualHighlight: '#ff6600',
};

function dpr() { return window.devicePixelRatio || 1; }

function resizeForHiDPI(canvas) {
  const r = canvas.getBoundingClientRect();
  const ratio = dpr();
  canvas.width = Math.max(1, Math.round(r.width * ratio));
  canvas.height = Math.max(1, Math.round(r.height * ratio));
  const ctx = canvas.getContext('2d');
  ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
  return { ctx, width: r.width, height: r.height };
}

function ma(values, window) {
  const out = new Array(values.length).fill(null);
  if (window <= 1 || values.length < window) return out;
  let sum = 0;
  for (let i = 0; i < values.length; i++) {
    sum += values[i];
    if (i >= window) sum -= values[i - window];
    if (i >= window - 1) out[i] = sum / window;
  }
  return out;
}

function niceScale(min, max) {
  if (!isFinite(min) || !isFinite(max)) return { min: 0, max: 1 };
  if (min === max) {
    const pad = Math.abs(min) > 0 ? Math.abs(min) * 0.1 : 0.01;
    return { min: min - pad, max: max + pad };
  }
  const pad = (max - min) * 0.08;
  return { min: min - pad, max: max + pad };
}

export function renderCandleChart(canvas, chartData, opts = {}) {
  const { ctx, width, height } = resizeForHiDPI(canvas);
  const { frozen = [], live = null } = chartData;

  const candles = [];
  frozen.forEach(f => candles.push({ key: f.bucket_key, stats: f.stats, live: false }));
  if (live) candles.push({ key: live.bucket_key, stats: live.stats, live: true });

  if (candles.length === 0) {
    ctx.fillStyle = '#888';
    ctx.font = '11px "MS Sans Serif"';
    ctx.fillText('(no data)', 8, 18);
    return;
  }

  // Determine Y range from whiskers + outliers + extremes + regression band if present.
  let ymin = +Infinity, ymax = -Infinity;
  candles.forEach(c => {
    const s = c.stats;
    ymin = Math.min(ymin, s.whisker_low ?? s.q1);
    ymax = Math.max(ymax, s.whisker_high ?? s.q3);
    (s.outliers || []).forEach(v => { ymin = Math.min(ymin, v); ymax = Math.max(ymax, v); });
    (s.extremes || []).forEach(v => { ymin = Math.min(ymin, v); ymax = Math.max(ymax, v); });
  });
  if (opts.regression) {
    (opts.regression.band_upper || []).forEach(v => ymax = Math.max(ymax, v));
    (opts.regression.band_lower || []).forEach(v => ymin = Math.min(ymin, v));
  }
  const ys = niceScale(ymin, ymax);

  const padL = 46, padR = 8, padT = 8, padB = 22;
  const plotW = Math.max(1, width - padL - padR);
  const plotH = Math.max(1, height - padT - padB);
  const yTo = v => padT + plotH - (v - ys.min) / (ys.max - ys.min) * plotH;

  // Axis + gridlines
  ctx.strokeStyle = COLORS.grid;
  ctx.lineWidth = 1;
  const gridSteps = 4;
  ctx.font = '10px "Courier New"';
  ctx.fillStyle = '#444';
  ctx.textAlign = 'right';
  ctx.textBaseline = 'middle';
  for (let i = 0; i <= gridSteps; i++) {
    const v = ys.min + (ys.max - ys.min) * i / gridSteps;
    const y = yTo(v);
    ctx.beginPath();
    ctx.moveTo(padL, y);
    ctx.lineTo(padL + plotW, y);
    ctx.stroke();
    ctx.fillText(v.toFixed(3), padL - 3, y);
  }
  // Y-axis line
  ctx.strokeStyle = COLORS.axis;
  ctx.beginPath();
  ctx.moveTo(padL, padT);
  ctx.lineTo(padL, padT + plotH);
  ctx.lineTo(padL + plotW, padT + plotH);
  ctx.stroke();

  // X slots
  const n = candles.length;
  const slotW = plotW / n;
  const bodyW = Math.max(
    CANDLE_WIDTH_MIN,
    Math.min(CANDLE_WIDTH_MAX, slotW * CANDLE_WIDTH_RATIO)
  );

  // Prediction band (drawn under candles)
  if (opts.regression) {
    const bu = opts.regression.band_upper || [];
    const bl = opts.regression.band_lower || [];
    if (bu.length === candles.length && bl.length === candles.length) {
      ctx.fillStyle = COLORS.band;
      ctx.beginPath();
      for (let i = 0; i < candles.length; i++) {
        const x = padL + slotW * (i + 0.5);
        const y = yTo(bu[i]);
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
      }
      for (let i = candles.length - 1; i >= 0; i--) {
        const x = padL + slotW * (i + 0.5);
        ctx.lineTo(x, yTo(bl[i]));
      }
      ctx.closePath();
      ctx.fill();
    }
  }

  const highlighted = new Set(opts.regression?.highlighted_bucket_keys || []);

  // Candles
  candles.forEach((c, i) => {
    const x = padL + slotW * (i + 0.5);
    const s = c.stats;
    const yQ1 = yTo(s.q1);
    const yQ3 = yTo(s.q3);
    const yMed = yTo(s.median);
    const yHigh = yTo(s.whisker_high);
    const yLow = yTo(s.whisker_low);

    // Whisker vertical + caps
    ctx.strokeStyle = COLORS.whisker;
    ctx.beginPath();
    ctx.moveTo(x, yHigh);
    ctx.lineTo(x, yLow);
    ctx.moveTo(x - bodyW / 3, yHigh);
    ctx.lineTo(x + bodyW / 3, yHigh);
    ctx.moveTo(x - bodyW / 3, yLow);
    ctx.lineTo(x + bodyW / 3, yLow);
    ctx.stroke();

    // Box body
    ctx.fillStyle = c.live ? COLORS.liveBody : COLORS.body;
    ctx.fillRect(x - bodyW / 2, yQ3, bodyW, Math.max(1, yQ1 - yQ3));
    ctx.strokeStyle = highlighted.has(c.key) ? COLORS.residualHighlight : COLORS.bodyStroke;
    ctx.lineWidth = highlighted.has(c.key) ? 2 : 1;
    ctx.strokeRect(x - bodyW / 2, yQ3, bodyW, Math.max(1, yQ1 - yQ3));
    ctx.lineWidth = 1;

    // Median
    ctx.strokeStyle = COLORS.median;
    ctx.beginPath();
    ctx.moveTo(x - bodyW / 2, yMed);
    ctx.lineTo(x + bodyW / 2, yMed);
    ctx.stroke();

    // Outliers & extremes
    ctx.fillStyle = COLORS.outlier;
    (s.outliers || []).forEach(v => {
      const yv = yTo(v);
      ctx.beginPath();
      ctx.arc(x, yv, 2, 0, Math.PI * 2);
      ctx.fill();
    });
    ctx.fillStyle = COLORS.extreme;
    (s.extremes || []).forEach(v => {
      const yv = yTo(v);
      ctx.beginPath();
      ctx.moveTo(x - 3, yv);
      ctx.lineTo(x + 3, yv);
      ctx.moveTo(x, yv - 3);
      ctx.lineTo(x, yv + 3);
      ctx.stroke();
    });
  });

  // Trend line (drawn above candles, below MA)
  if (opts.regression && (opts.regression.trend || []).length === candles.length) {
    ctx.strokeStyle = COLORS.trend;
    ctx.lineWidth = 2;
    ctx.beginPath();
    opts.regression.trend.forEach((v, i) => {
      const x = padL + slotW * (i + 0.5);
      const y = yTo(v);
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.stroke();
    ctx.lineWidth = 1;
  }

  // Moving averages over median sequence
  const medians = candles.map(c => c.stats.median);
  (opts.ma || [7]).forEach(w => {
    const line = ma(medians, w);
    ctx.strokeStyle = COLORS.ma;
    ctx.lineWidth = 1;
    ctx.beginPath();
    let started = false;
    line.forEach((v, i) => {
      if (v == null) return;
      const x = padL + slotW * (i + 0.5);
      const y = yTo(v);
      if (!started) { ctx.moveTo(x, y); started = true; }
      else ctx.lineTo(x, y);
    });
    ctx.stroke();
  });

  // X labels: pick stride + format so labels don't overlap.
  ctx.fillStyle = '#444';
  ctx.textAlign = 'center';
  ctx.textBaseline = 'top';
  ctx.font = '10px "Courier New"';
  const unit = opts.unit || 'day';
  const { format, stride } = pickLabelPlan(ctx, candles, slotW, unit);
  for (let i = 0; i < n; i++) {
    const forced = (i === 0 || i === n - 1);
    if (!forced && i % stride !== 0) continue;
    const label = format(candles[i].key);
    ctx.fillText(label, padL + slotW * (i + 0.5), padT + plotH + 4);
  }
}

function fmtFull(key) { return key; }
function fmtShort(key, unit) {
  if (unit === 'day') {
    // YYYY-MM-DD → MM-DD
    return key.length >= 10 ? key.slice(5) : key;
  }
  if (unit === 'week') {
    // YYYY-Www → Www
    const i = key.indexOf('-W');
    return i >= 0 ? key.slice(i + 1) : key;
  }
  if (unit === 'month') {
    // YYYY-MM → MM
    return key.length >= 7 ? key.slice(5) : key;
  }
  return key;
}

function pickLabelPlan(ctx, candles, slotW, unit) {
  if (candles.length === 0) return { format: fmtFull, stride: 1 };
  const sample = candles[Math.floor(candles.length / 2)].key;
  const fullW = ctx.measureText(fmtFull(sample)).width;
  const shortW = ctx.measureText(fmtShort(sample, unit)).width;

  // Try full first.
  const strideFull = Math.max(1, Math.ceil((fullW + X_LABEL_GAP_PX) / slotW));
  if (strideFull === 1) return { format: fmtFull, stride: 1 };

  // Fall back to short and recompute.
  const strideShort = Math.max(1, Math.ceil((shortW + X_LABEL_GAP_PX) / slotW));
  if (strideShort <= strideFull) {
    return { format: (k) => fmtShort(k, unit), stride: strideShort };
  }
  // short was somehow worse — stick with full at strideFull.
  return { format: fmtFull, stride: strideFull };
}
