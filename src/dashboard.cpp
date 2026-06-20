#include "dashboard.h"
#include "cluster.h"
#include "store.h"
#include "network.h"
#include "config.h"

#include <sys/stat.h>
#include <chrono>
#include <cstdint>

static std::string json_escape(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 8);
    for (char c : s) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n";  break;
            case '\r': out += "\\r";  break;
            case '\t': out += "\\t";  break;
            default:   out += c;
        }
    }
    return out;
}

static void json_kv(std::string& j, const std::string& k, const std::string& v, bool last = false) {
    j += "\"" + json_escape(k) + "\":\"" + json_escape(v) + "\"";
    if (!last) j += ",";
}

static void json_kv_int(std::string& j, const std::string& k, int64_t v, bool last = false) {
    j += "\"" + json_escape(k) + "\":" + std::to_string(v);
    if (!last) j += ",";
}

static void json_kv_uint(std::string& j, const std::string& k, uint64_t v, bool last = false) {
    j += "\"" + json_escape(k) + "\":" + std::to_string(v);
    if (!last) j += ",";
}

static void json_kv_bool(std::string& j, const std::string& k, bool v, bool last = false) {
    j += "\"" + json_escape(k) + "\":" + (v ? "true" : "false");
    if (!last) j += ",";
}

static std::string status_str(NodeStatus s) {
    switch (s) {
        case NodeStatus::ALIVE:   return "ALIVE";
        case NodeStatus::SUSPECT: return "SUSPECT";
        case NodeStatus::DEAD:    return "DEAD";
        case NodeStatus::LEFT:    return "LEFT";
    }
    return "UNKNOWN";
}

std::string collect_dashboard_json() {
    std::string j;
    j.reserve(4096);
    j += "{";

    // self
    j += "\"self\":{";
    json_kv(j, "id", std::to_string(self_node.id));
    json_kv(j, "ip", self_node.ip);
    json_kv_int(j, "port", self_node.port);
    json_kv_uint(j, "generation", self_node.generation, true);
    j += "},";

    // cluster
    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    j += "\"cluster\":{";
    json_kv_int(j, "active_node_count", static_cast<int64_t>(get_active_node_count()));
    j += "\"nodes\":{";
    bool first = true;
    for (const auto& [nid, status] : cluster_state) {
        if (!first) j += ",";
        first = false;
        j += "\"" + std::to_string(nid.id) + "\":{";
        json_kv(j, "ip", nid.ip);
        json_kv_int(j, "port", nid.port);
        json_kv_uint(j, "generation", nid.generation);
        json_kv(j, "status", status_str(status));
        auto ls = last_seen.find(nid);
        if (ls != last_seen.end())
            json_kv_int(j, "last_seen_ms_ago", now - ls->second, true);
        else
            json_kv(j, "last_seen_ms_ago", "null", true);
        j += "}";
    }
    j += "},";
    json_kv_bool(j, "self_is_alive", cluster_state.count(self_node) &&
                 cluster_state.at(self_node) == NodeStatus::ALIVE, true);
    j += "},";

    // ring
    j += "\"ring\":{";
    json_kv_uint(j, "version", get_ring_version());
    json_kv_uint(j, "size", static_cast<uint64_t>(ring.size()), true);
    j += "},";

    // store
    int64_t aof_size = 0;
    struct stat st;
    if (stat("data/append_only.aof", &st) == 0)
        aof_size = st.st_size;

    j += "\"store\":{";
    json_kv_uint(j, "key_count", static_cast<uint64_t>(STORE.size()));
    json_kv_int(j, "memory_usage_bytes", g_current_memory_usage);
    json_kv_int(j, "maxmemory_bytes", g_maxmemory_bytes);
    json_kv_uint(j, "version", g_store_version.load(), true);
    j += "},";

    // pending
    j += "\"pending\":{";
    json_kv_uint(j, "replica_queue", static_cast<uint64_t>(replica_queue.size()));
    json_kv_uint(j, "pending_writes", static_cast<uint64_t>(pending_writes.size()));
    json_kv_uint(j, "pending_reads", static_cast<uint64_t>(pending_reads.size()));
    json_kv_uint(j, "pending_gathers", static_cast<uint64_t>(pending_gathers.size()), true);
    j += "},";

    // AOF
    j += "\"aof\":{";
    json_kv_int(j, "size_bytes", aof_size, true);
    j += "},";

    // config
    const auto& cfg = config();
    j += "\"config\":{";
    json_kv_int(j, "client_port", cfg.client_port);
    json_kv_int(j, "cluster_port", cfg.effective_cluster_port());
    json_kv_int(j, "dashboard_port", cfg.effective_dashboard_port());
    json_kv_int(j, "replication_factor", cfg.replication_factor);
    json_kv_int(j, "read_quorum", cfg.read_quorum);
    json_kv_int(j, "write_quorum", cfg.write_quorum);
    json_kv_int(j, "gossip_interval_ms", cfg.gossip_interval_ms);
    json_kv_int(j, "failure_timeout_ms", cfg.failure_timeout_ms);
    json_kv_int(j, "dead_timeout_multiplier", cfg.dead_timeout_multiplier);
    json_kv_int(j, "tombstone_ttl_ms", cfg.tombstone_ttl_ms);
    json_kv_int(j, "merkle_interval_ms", cfg.merkle_interval_ms);
    json_kv_int(j, "maxmemory_bytes", cfg.maxmemory_bytes, true);
    j += "}";

    j += "}";
    return j;
}
