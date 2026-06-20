#pragma once
#ifndef DREDIS_DASHBOARD_H
#define DREDIS_DASHBOARD_H

#include <string>

std::string collect_dashboard_json();

// Embedded dashboard.html — served at GET / and GET /dashboard.html.
// Edit this string and recompile; no external file dependency.
static const char DASHBOARD_HTML[] = R"html(
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>DRedis Dashboard</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0d1117; color: #c9d1d9; padding: 24px; }
  h1 { color: #58a6ff; margin-bottom: 16px; font-size: 24px; }
  h2 { color: #8b949e; font-size: 16px; margin-bottom: 8px; border-bottom: 1px solid #21262d; padding-bottom: 4px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 16px; margin-bottom: 24px; }
  .card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; }
  .card .label { color: #8b949e; font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px; }
  .card .value { font-size: 28px; font-weight: 600; margin-top: 4px; }
  .card .value.green { color: #3fb950; }
  .card .value.yellow { color: #d29922; }
  .card .value.red { color: #f85149; }
  .card .value.blue { color: #58a6ff; }
  table { width: 100%; border-collapse: collapse; margin-top: 8px; }
  th { text-align: left; color: #8b949e; font-size: 12px; text-transform: uppercase; padding: 6px 8px; border-bottom: 1px solid #30363d; }
  td { padding: 6px 8px; border-bottom: 1px solid #21262d; font-size: 14px; }
  .status-badge { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 12px; font-weight: 500; }
  .status-ALIVE { background: #0d5320; color: #7ee787; }
  .status-SUSPECT { background: #5a3e00; color: #d29922; }
  .status-DEAD { background: #490202; color: #f85149; }
  .status-LEFT { background: #21262d; color: #8b949e; }
  .error { color: #f85149; padding: 24px; text-align: center; }
  .loading { color: #8b949e; padding: 24px; text-align: center; }
</style>
</head>
<body>
<h1>DRedis Dashboard</h1>
<div id="error" class="error" style="display:none"></div>
<div id="loading" class="loading">Loading&hellip;</div>
<div id="content" style="display:none">

  <div class="grid" id="summary-cards"></div>

  <h2>Cluster Nodes</h2>
  <div class="card"><table><thead><tr>
    <th>ID</th><th>IP</th><th>Port</th><th>Status</th><th>Last Seen (ms ago)</th>
  </tr></thead><tbody id="nodes-tbody"></tbody></table></div>

  <h2>Ring</h2>
  <div class="card" id="ring-card"></div>

  <h2>Store</h2>
  <div class="card" id="store-card"></div>

  <h2>Replication &amp; Pending Ops</h2>
  <div class="card" id="pending-card"></div>

  <h2>AOF</h2>
  <div class="card" id="aof-card"></div>

  <h2>Config</h2>
  <div class="card" id="config-card"></div>

</div>
<script>
async function refresh(){
  const errEl = document.getElementById('error');
  const loadEl = document.getElementById('loading');
  const contEl = document.getElementById('content');
  try {
    const r = await fetch('/metrics.json');
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const d = await r.json();
    errEl.style.display = 'none';
    contEl.style.display = 'block';
    loadEl.style.display = 'none';
    render(d);
  } catch(e){
    errEl.style.display = 'block';
    errEl.textContent = 'Error: ' + e.message;
    contEl.style.display = 'none';
    loadEl.style.display = 'none';
  }
}

function render(d){
  // Summary cards
  const cards = document.getElementById('summary-cards');
  cards.innerHTML = '';
  const summaries = [
    {label:'Keys', value:d.store.key_count, cls:'blue'},
    {label:'Memory', value:fmtBytes(d.store.memory_usage_bytes)+' / '+fmtBytes(d.store.maxmemory_bytes), cls:''},
    {label:'Memory %', value:(d.store.memory_usage_bytes/d.store.maxmemory_bytes*100).toFixed(1)+'%', cls:d.store.memory_usage_bytes/d.store.maxmemory_bytes>0.8?'red':'green'},
    {label:'Self Node', value:d.self.id, cls:'blue'}, // d.self.id is now a JSON string (safe)
    {label:'Active Nodes', value:d.cluster.active_node_count, cls:'green'},
    {label:'Ring Version', value:d.ring.version, cls:'blue'},
    {label:'Ring Size', value:d.ring.size, cls:''},
    {label:'Store Version', value:d.store.version, cls:''},
    {label:'AOF Size', value:fmtBytes(d.aof.size_bytes), cls:''},
    {label:'Replica Queue', value:d.pending.replica_queue, cls:d.pending.replica_queue>0?'yellow':'green'},
    {label:'Pending Writes', value:d.pending.pending_writes, cls:d.pending.pending_writes>0?'yellow':'green'},
    {label:'Pending Reads', value:d.pending.pending_reads, cls:d.pending.pending_reads>0?'yellow':'green'},
    {label:'Pending Gathers', value:d.pending.pending_gathers, cls:d.pending.pending_gathers>0?'yellow':'green'},
  ];
  for(const s of summaries){
    const c = document.createElement('div'); c.className = 'card';
    c.innerHTML = '<div class="label">'+s.label+'</div><div class="value'+(s.cls?' '+s.cls:'')+'">'+s.value+'</div>';
    cards.appendChild(c);
  }

  // Nodes table
  const tbody = document.getElementById('nodes-tbody');
  tbody.innerHTML = '';
  const now = Date.now();
  for(const nid in d.cluster.nodes){
    const n = d.cluster.nodes[nid];
    const tr = document.createElement('tr');
    const age = n.last_seen_ms_ago != null ? n.last_seen_ms_ago + 'ms' : '-';
    tr.innerHTML = '<td>'+esc(nid)+'</td><td>'+esc(n.ip)+'</td><td>'+esc(n.port)+'</td><td><span class="status-badge status-'+n.status+'">'+n.status+'</span></td><td>'+age+'</td>';
    tbody.appendChild(tr);
  }

  // Ring
  document.getElementById('ring-card').innerHTML =
    'Version: <b>'+d.ring.version+'</b><br>Size: <b>'+d.ring.size+'</b> vnodes (150 per active node)';

  // Store
  document.getElementById('store-card').innerHTML =
    'Keys: <b>'+d.store.key_count+'</b><br>'+
    'Memory: <b>'+fmtBytes(d.store.memory_usage_bytes)+'</b> / <b>'+fmtBytes(d.store.maxmemory_bytes)+'</b><br>'+
    'Store Version: <b>'+d.store.version+'</b>';

  // Pending
  document.getElementById('pending-card').innerHTML =
    'Replica Queue: <b>'+d.pending.replica_queue+'</b><br>'+
    'Pending Writes: <b>'+d.pending.pending_writes+'</b><br>'+
    'Pending Reads: <b>'+d.pending.pending_reads+'</b><br>'+
    'Pending Gathers: <b>'+d.pending.pending_gathers+'</b>';

  // AOF
  document.getElementById('aof-card').innerHTML =
    'File Size: <b>'+fmtBytes(d.aof.size_bytes)+'</b>';

  // Config
  const cfg = d.config;
  document.getElementById('config-card').innerHTML =
    'Client Port: <b>'+cfg.client_port+'</b><br>'+
    'Cluster Port: <b>'+cfg.cluster_port+'</b><br>'+
    'Dashboard Port: <b>'+cfg.dashboard_port+'</b><br>'+
    'Replication Factor: <b>'+cfg.replication_factor+'</b><br>'+
    'Read Quorum: <b>'+cfg.read_quorum+'</b> / Write Quorum: <b>'+cfg.write_quorum+'</b><br>'+
    'Gossip Interval: <b>'+cfg.gossip_interval_ms+'ms</b><br>'+
    'Failure Timeout: <b>'+cfg.failure_timeout_ms+'ms</b><br>'+
    'Tombstone TTL: <b>'+cfg.tombstone_ttl_ms+'ms</b>';
}

function fmtBytes(b){
  if(b===0) return '0 B';
  const u=['B','KB','MB','GB','TB'];
  let i=0; let v=b;
  while(v>=1024 && i<u.length-1){ v/=1024; i++; }
  return v.toFixed(1)+' '+u[i];
}

function esc(s){ return String(s).replace(/[&<>"']/g,function(m){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m];}); }

setInterval(refresh, 2000);
refresh();
</script>
</body>
</html>
)html";

#endif
