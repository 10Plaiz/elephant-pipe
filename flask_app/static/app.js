// Polls /api/data every 10 s and renders the rows stored in Cassandra.
const REFRESH_MS = 10000;
const $ = (id) => document.getElementById(id);
let seen = new Set();          // record keys already shown, to highlight new rows

const usd = (v, min = 0, max = 2) => v == null ? "-" :
  "$" + Number(v).toLocaleString("en-US", { minimumFractionDigits: min, maximumFractionDigits: max });
const price = (v) => v == null ? "-" : (Math.abs(v) < 1 ? usd(v, 4, 6) : usd(v, 2, 2));
const big = (v) => {
  if (v == null) return "-";
  const a = Math.abs(v);
  if (a >= 1e12) return "$" + (v / 1e12).toFixed(2) + "T";
  if (a >= 1e9) return "$" + (v / 1e9).toFixed(2) + "B";
  if (a >= 1e6) return "$" + (v / 1e6).toFixed(2) + "M";
  return usd(v);
};
const pct = (v) => v == null ? "-" : (v > 0 ? "+" : "") + Number(v).toFixed(2) + "%";
const cls = (v) => v > 0 ? "up" : v < 0 ? "down" : "";
const time = (iso) => iso ? new Date(iso).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }) : "-";
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

function ago(iso) {
  if (!iso) return "no writes yet";
  const s = Math.max(0, Math.round((Date.now() - new Date(iso)) / 1000));
  return s < 60 ? `${s}s ago` : `${Math.round(s / 60)} min ago`;
}

function renderStatus(d) {
  const st = $("status");
  st.className = "status";
  st.innerHTML = `<span class="dot" aria-hidden="true"></span>` +
    `Last Spark write ${ago(d.last_write)}. Stored today: ${d.counts.market_prices.toLocaleString()} price rows, ` +
    `${d.counts.global_stats.toLocaleString()} market snapshots. Page refreshed ${time(d.server_time)}.`;
}

function renderTicker(latest) {
  if (!latest.length) { $("ticker").innerHTML = "<span>Waiting for the first rows from Spark</span>"; return; }
  const items = latest.map((r) =>
    `<span><b>${esc(r.symbol)}</b>${price(r.current_price)} <em class="${r.trend === "UP" ? "up" : r.trend === "DOWN" ? "down" : ""}" style="font-style:normal">${pct(r.price_change_pct_24h)}</em></span>`
  ).join("");
  $("ticker").innerHTML = items + items;   // doubled for a seamless loop
}

function renderStats(g) {
  if (!g) return;
  $("s-cap").textContent = big(g.total_market_cap_usd);
  $("s-vol").textContent = big(g.total_volume_usd);
  $("s-btc").textContent = g.btc_dominance_pct == null ? "-" : g.btc_dominance_pct.toFixed(2) + "%";
  $("s-eth").textContent = g.eth_dominance_pct == null ? "-" : g.eth_dominance_pct.toFixed(2) + "%";
  $("s-chg").innerHTML = `<span class="${cls(g.market_cap_change_pct_24h)}">${pct(g.market_cap_change_pct_24h)}</span>`;
  const sent = g.market_sentiment || "-";
  $("s-sent").innerHTML = `<span class="${sent === "BULLISH" ? "up" : sent === "BEARISH" ? "down" : ""}">${esc(sent.charAt(0) + sent.slice(1).toLowerCase())}</span>`;
}

function emptyRow(cols, msg) { return `<tr class="empty"><td colspan="${cols}">${msg}</td></tr>`; }

function renderLatest(latest) {
  $("latest-meta").textContent = latest.length ? `${latest.length} coins, captured ${time(latest[0].captured_at)}` : "";
  $("latest").innerHTML = latest.length ? latest.map((r) => `
    <tr>
      <td>${r.market_cap_rank ?? "-"}</td>
      <td class="l"><span class="sym">${esc(r.symbol)}</span><span class="nm">${esc(r.name)}</span></td>
      <td>${price(r.current_price)}</td>
      <td class="${cls(r.price_change_pct_24h)}">${pct(r.price_change_pct_24h)}</td>
      <td>${price(r.high_24h)}</td>
      <td>${price(r.low_24h)}</td>
      <td>${r.volatility_pct_24h == null ? "-" : r.volatility_pct_24h.toFixed(2) + "%"}</td>
      <td>${big(r.total_volume)}</td>
      <td>${big(r.market_cap)}</td>
    </tr>`).join("") : emptyRow(9, "No price rows yet. Start Producer 1 and the Spark job.");
}

function renderGlobal(hist) {
  $("global-meta").textContent = hist.length ? `${hist.length} snapshots` : "";
  $("global").innerHTML = hist.length ? hist.map((g) => `
    <tr>
      <td class="l">${time(g.captured_at)}</td>
      <td>${big(g.total_market_cap_usd)}</td>
      <td>${g.btc_dominance_pct == null ? "-" : g.btc_dominance_pct.toFixed(2) + "%"}</td>
      <td class="${cls(g.market_cap_change_pct_24h)}">${pct(g.market_cap_change_pct_24h)}</td>
      <td class="l">${esc(g.market_sentiment)}</td>
    </tr>`).join("") : emptyRow(5, "No market snapshots yet. Start Producer 2 and the Spark job.");
}

function renderRecords(records) {
  $("records-meta").textContent = records.length ? `Newest ${records.length} rows, newest first` : "";
  const next = new Set();
  $("records").innerHTML = records.length ? records.map((r) => {
    const key = r.captured_at + r.coin_id;
    next.add(key);
    const fresh = seen.size && !seen.has(key) ? ' class="fresh"' : "";
    return `
    <tr${fresh}>
      <td class="l">${time(r.captured_at)}</td>
      <td class="l">${time(r.processed_at)}</td>
      <td class="l"><span class="sym">${esc(r.symbol)}</span><span class="nm">${esc(r.coin_id)}</span></td>
      <td>${price(r.current_price)}</td>
      <td class="${cls(r.price_change_pct_24h)}">${pct(r.price_change_pct_24h)}</td>
      <td class="l ${r.trend === "UP" ? "up" : r.trend === "DOWN" ? "down" : ""}">${esc(r.trend)}</td>
      <td class="l">${esc(r.capture_date)}</td>
    </tr>`;
  }).join("") : emptyRow(7, "No stored rows yet.");
  seen = next;
}

async function refresh() {
  try {
    const res = await fetch("/api/data", { cache: "no-store" });
    const d = await res.json();
    if (!d.ok) throw new Error(d.error);
    renderStatus(d);
    renderTicker(d.latest);
    renderStats(d.global_latest);
    renderLatest(d.latest);
    renderGlobal(d.global_history);
    renderRecords(d.records);
  } catch (e) {
    const st = $("status");
    st.className = "status err";
    st.innerHTML = `<span class="dot" aria-hidden="true"></span>${esc(e.message)}. Check that Cassandra is running (docker compose ps). Retrying in 10 seconds.`;
  }
}

refresh();
setInterval(refresh, REFRESH_MS);
