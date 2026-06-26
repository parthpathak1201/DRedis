#include "cluster.h"
#include "config.h"
#include "crc32c.h"
#include "network.h"
#include <iostream>
#include <unordered_set>
#include <chrono>
#include <algorithm>
#include <random>
#include <cstring>
#include <arpa/inet.h>

const int VNODE_COUNT = 150;
const int RECOVERY_QUORUM = 3;

NodeID self_node;
std::unordered_map<NodeID, NodeStatus> cluster_state;
std::map<uint64_t, NodeID> ring;
std::unordered_map<NodeID, int64_t> last_seen;
std::unordered_map<NodeID, int64_t> suspect_since;
std::unordered_map<uint64_t, uint64_t> node_versions;
std::unordered_map<uint64_t, int> recovery_count;
uint64_t self_version = 0;
uint64_t ring_version = 0;
size_t active_node_count = 0;
NodeID slot_owners[16384] = {};

static const uint16_t crc16tab[256] = {
    0x0000,0x1021,0x2042,0x3063,0x4084,0x50a5,0x60c6,0x70e7,
    0x8108,0x9129,0xa14a,0xb16b,0xc18c,0xd1ad,0xe1ce,0xf1ef,
    0x1231,0x0210,0x3273,0x2252,0x52b5,0x4294,0x72f7,0x62d6,
    0x9339,0x8318,0xb37b,0xa35a,0xd3bd,0xc39c,0xf3ff,0xe3de,
    0x2462,0x3443,0x0420,0x1401,0x64e6,0x74c7,0x44a4,0x5485,
    0xa56a,0xb54b,0x8528,0x9509,0xe5ee,0xf5cf,0xc5ac,0xd58d,
    0x3653,0x2672,0x1611,0x0630,0x76d7,0x66f6,0x5695,0x46b4,
    0xb75b,0xa77a,0x9719,0x8738,0xf7df,0xe7fe,0xd79d,0xc7bc,
    0x48c4,0x58e5,0x6886,0x78a7,0x0840,0x1861,0x2802,0x3823,
    0xc9cc,0xd9ed,0xe98e,0xf9af,0x8948,0x9969,0xa90a,0xb92b,
    0x5af5,0x4ad4,0x7ab7,0x6a96,0x1a71,0x0a50,0x3a33,0x2a12,
    0xdbfd,0xcbdc,0xfbbf,0xeb9e,0x9b79,0x8b58,0xbb3b,0xab1a,
    0x6ca6,0x7c87,0x4ce4,0x5cc5,0x2c22,0x3c03,0x0c60,0x1c41,
    0xedae,0xfd8f,0xcdec,0xddcd,0xad2a,0xbd0b,0x8d68,0x9d49,
    0x7e97,0x6eb6,0x5ed5,0x4ef4,0x3e13,0x2e32,0x1e51,0x0e70,
    0xff9f,0xefbe,0xdfdd,0xcffc,0xbf1b,0xaf3a,0x9f59,0x8f78,
    0x9188,0x81a9,0xb1ca,0xa1eb,0xd10c,0xc12d,0xf14e,0xe16f,
    0x1080,0x00a1,0x30c2,0x20e3,0x5004,0x4025,0x7046,0x6067,
    0x83b9,0x9398,0xa3fb,0xb3da,0xc33d,0xd31c,0xe37f,0xf35e,
    0x02b1,0x1290,0x22f3,0x32d2,0x4235,0x5214,0x6277,0x7256,
    0xb5ea,0xa5cb,0x95a8,0x8589,0xf56e,0xe54f,0xd52c,0xc50d,
    0x34e2,0x24c3,0x14a0,0x0481,0x7466,0x6447,0x5424,0x4405,
    0xa7db,0xb7fa,0x8799,0x97b8,0xe75f,0xf77e,0xc71d,0xd73c,
    0x26d3,0x36f2,0x0691,0x16b0,0x6657,0x7676,0x4615,0x5634,
    0xd94c,0xc96d,0xf90e,0xe92f,0x99c8,0x89e9,0xb98a,0xa9ab,
    0x5844,0x4865,0x7806,0x6827,0x18c0,0x08e1,0x3882,0x28a3,
    0xcb7d,0xdb5c,0xeb3f,0xfb1e,0x8bf9,0x9bd8,0xabbb,0xbb9a,
    0x4a75,0x5a54,0x6a37,0x7a16,0x0af1,0x1ad0,0x2ab3,0x3a92,
    0xfd2e,0xed0f,0xdd6c,0xcd4d,0xbdaa,0xad8b,0x9de8,0x8dc9,
    0x7c26,0x6c07,0x5c64,0x4c45,0x3ca2,0x2c83,0x1ce0,0x0cc1,
    0xef1f,0xff3e,0xcf5d,0xdf7c,0xaf9b,0xbfba,0x8fd9,0x9ff8,
    0x6e17,0x7e36,0x4e55,0x5e74,0x2e93,0x3eb2,0x0ed1,0x1ef0
};

uint16_t crc16(const std::string &s) {
    uint16_t crc = 0;
    for (unsigned char c : s)
        crc = (crc << 8) ^ crc16tab[((crc >> 8) ^ c) & 0xFF];
    return crc;
}

static void build_ring() {
    ring.clear();
    active_node_count = 0;
    for (const auto &[node, status]: cluster_state) {
        if (status == NodeStatus::ALIVE || status == NodeStatus::SUSPECT) {
            active_node_count++;
            for (int i = 0; i < VNODE_COUNT; ++i) {
                std::string vnode_key = std::to_string(node.id) + "#" + std::to_string(i);
                uint64_t token = hash_token(vnode_key);
                ring[token] = node;
            }
        }
    }
    // Build slot ownership table for CRC16-based key lookup
    for (size_t i = 0; i < 16384; i++) {
        uint64_t t = hash_token(std::to_string(i));
        auto it = ring.lower_bound(t);
        if (it == ring.end()) it = ring.begin();
        slot_owners[i] = it->second;
    }
    ring_version++;
}

void update_last_seen(const NodeID &node) {
    auto it = cluster_state.find(node);
    if (it != cluster_state.end()) {
        if (it->second == NodeStatus::SUSPECT) {
            it->second = NodeStatus::ALIVE;
            node_versions[node.id] = ++self_version;
            suspect_since.erase(node);
            build_ring();
        } else if (it->second == NodeStatus::DEAD) {
            // Only resurrect from DEAD if TCP connection is alive — gossip alone is stale
            auto pit = peers_by_node_id.find(node.id);
            if (pit == peers_by_node_id.end() || pit->second.fd < 0)
                return;
            recovery_count[node.id]++;
            if (recovery_count[node.id] >= RECOVERY_QUORUM) {
                it->second = NodeStatus::ALIVE;
                node_versions[node.id] = ++self_version;
                recovery_count.erase(node.id);
                build_ring();
            }
        }
    }
    last_seen[node] = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
}

void init_cluster(NodeID self, const std::vector<NodeID> &) {
    self_node = self;
    cluster_state[self_node] = NodeStatus::ALIVE;
    update_last_seen(self_node);
    build_ring();
}

uint64_t hash_token(const std::string &key) {
    return crc32c_64(key.data(), key.size());
}

void add_node(const NodeID &node) {
    auto it = cluster_state.find(node);
    if (it != cluster_state.end() && it->second != NodeStatus::DEAD && it->second != NodeStatus::LEFT) {
        return;
    }
    cluster_state[node] = NodeStatus::ALIVE;
    update_last_seen(node);
    node_versions[node.id] = ++self_version;
    build_ring();
}

void resolve_node_address(const std::string& ip, uint16_t port, uint64_t real_id) {
    for (auto it = cluster_state.begin(); it != cluster_state.end(); ) {
        if (it->first.id != real_id && it->first.id != self_node.id &&
            it->first.ip == ip && it->first.port == port) {
            it = cluster_state.erase(it);
        } else {
            ++it;
        }
    }
    node_versions[self_node.id] = ++self_version;
    build_ring();
}

void remove_node(const NodeID &node) {
    auto it = cluster_state.find(node);
    if (it == cluster_state.end() || it->second == NodeStatus::LEFT) {
        return;
    }
    it->second = NodeStatus::LEFT;
    last_seen.erase(node);
    node_versions[node.id] = ++self_version;
    build_ring();
}

void mark_dead(const NodeID &node) {
    auto it = cluster_state.find(node);
    if (it == cluster_state.end() || it->second == NodeStatus::DEAD) {
        return;
    }
    it->second = NodeStatus::DEAD;
    last_seen.erase(node);
    suspect_since.erase(node);
    recovery_count.erase(node.id);
    node_versions[node.id] = ++self_version;
}

void mark_suspect(const NodeID &node) {
    auto it = cluster_state.find(node);
    if (it == cluster_state.end() || it->second != NodeStatus::ALIVE) {
        return;
    }
    it->second = NodeStatus::SUSPECT;
    node_versions[node.id] = ++self_version;
}

void check_timeouts() {
    int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

    std::vector<NodeID> nodes_to_mark_suspect;
    std::vector<NodeID> nodes_to_mark_dead;

    for (auto const& [node, last_ts] : last_seen) {
        if (node.id == self_node.id) { last_seen[node] = now; continue; }
        auto it = cluster_state.find(node);
        if (it == cluster_state.end()) continue;

        if (it->second == NodeStatus::ALIVE && (now - last_ts > config().failure_timeout_ms)) {
            nodes_to_mark_suspect.push_back(node);
        } else if (it->second == NodeStatus::SUSPECT) {
            auto st = suspect_since.find(node);
            int64_t suspect_ts = (st != suspect_since.end()) ? st->second : last_ts;
            if (now - suspect_ts > static_cast<int64_t>(config().failure_timeout_ms) * config().dead_timeout_multiplier)
                nodes_to_mark_dead.push_back(node);
        }
    }

    bool ring_dirty = false;
    for (const auto& node : nodes_to_mark_suspect) {
        mark_suspect(node);
        suspect_since[node] = now;
        ring_dirty = true;
    }
    for (const auto& node : nodes_to_mark_dead) {
        mark_dead(node);
        ring_dirty = true;
    }
    if (ring_dirty) {
        build_ring();
    }
}

NodeID get_owner(const std::string &key) {
    if (ring.empty()) return self_node;
    uint16_t slot = crc16(key) & 0x3FFF;
    if (slot_owners[slot].id != 0)
        return slot_owners[slot];
    uint64_t token = hash_token(key);
    auto it = ring.lower_bound(token);
    if (it == ring.end()) {
        it = ring.begin();
    }
    return it->second;
}

std::vector<NodeID> get_replicas(const std::string &key) {
    std::vector<NodeID> res;
    if (ring.empty()) return res;

    NodeID owner = get_owner(key);
    std::unordered_set<uint64_t> found_ids;

    auto it = ring.begin();
    for (auto &[tok, node] : ring) {
        if (node.id == owner.id) {
            it = ring.find(tok);
            break;
        }
    }

    const size_t target_count = std::min((size_t)config().replication_factor, active_node_count);
    if (target_count == 0) return res;

    auto start_it = it;
    bool wrapped = false;

    while (res.size() < target_count) {
        if (it == ring.end()) {
            it = ring.begin();
            wrapped = true;
        }
        if (wrapped && it == start_it) break;
        const NodeID &node = it->second;
        if (found_ids.find(node.id) == found_ids.end()) {
            res.push_back(node);
            found_ids.insert(node.id);
        }
        it++;
    }

    return res;
}

std::vector<NodeID> get_all_nodes() {
    std::vector<NodeID> res;
    for (const auto &[node, status]: cluster_state) {
        if (status == NodeStatus::ALIVE || status == NodeStatus::SUSPECT) {
            res.push_back(node);
        }
    }
    return res;
}

uint64_t get_ring_version() {
    return ring_version;
}

size_t get_active_node_count() {
    return active_node_count;
}

std::string encode_gossip_payload() {
    std::vector<NodeID> candidates;
    // Always include self_node so peers know our latest status
    candidates.push_back(self_node);
    for (const auto& [node, status] : cluster_state) {
        if (node.id != self_node.id)
            candidates.push_back(node);
    }
    if (candidates.empty()) return {};

    // Shuffle and pick up to 4 (3 peers + self)
    static std::mt19937 rng(std::random_device{}());
    std::shuffle(candidates.begin() + 1, candidates.end(), rng);
    uint32_t count = static_cast<uint32_t>(std::min(candidates.size(), size_t(GOSSIP_FANOUT + 1))); // +1 for self

    std::string payload;
    payload.append(reinterpret_cast<const char*>(&count), sizeof(uint32_t));
    for (size_t i = 0; i < count; i++) {
        const auto& node = candidates[i];
        auto it = cluster_state.find(node);
        uint8_t status_byte = it != cluster_state.end() ? static_cast<uint8_t>(it->second) : 0;
        uint64_t gen = node.generation;
        auto vit = node_versions.find(node.id);
        uint64_t ver = vit != node_versions.end() ? vit->second : 0;
        uint16_t ip_len = static_cast<uint16_t>(node.ip.size());
        uint16_t net_ip_len = htons(ip_len);

        payload.append(reinterpret_cast<const char*>(&node.id), sizeof(uint64_t));
        payload.append(reinterpret_cast<const char*>(&net_ip_len), sizeof(uint16_t));
        payload.append(node.ip);
        uint16_t net_port = htons(node.port);
        payload.append(reinterpret_cast<const char*>(&net_port), sizeof(uint16_t));
        payload.push_back(status_byte);
        payload.append(reinterpret_cast<const char*>(&gen), sizeof(uint64_t));
        payload.append(reinterpret_cast<const char*>(&ver), sizeof(uint64_t));
    }
    return payload;
}

void apply_gossip_payload(std::string_view payload) {
    if (payload.size() < sizeof(uint32_t)) return;
    uint32_t count;
    memcpy(&count, payload.data(), sizeof(uint32_t));
    size_t offset = sizeof(uint32_t);

    for (uint32_t i = 0; i < count; i++) {
        if (offset + sizeof(uint64_t) + 1 > payload.size()) break;

        uint64_t id;
        memcpy(&id, payload.data() + offset, sizeof(uint64_t));
        offset += sizeof(uint64_t);

        if (offset + sizeof(uint16_t) > payload.size()) break;
        uint16_t net_ip_len;
        memcpy(&net_ip_len, payload.data() + offset, sizeof(uint16_t));
        offset += sizeof(uint16_t);
        uint16_t ip_len = ntohs(net_ip_len);
        if (offset + ip_len + sizeof(uint16_t) + 1 > payload.size()) break;

        std::string ip(payload.data() + offset, ip_len);
        offset += ip_len;

        uint16_t net_port;
        memcpy(&net_port, payload.data() + offset, sizeof(uint16_t));
        offset += sizeof(uint16_t);
        uint16_t port = ntohs(net_port);

        uint8_t status_byte = static_cast<uint8_t>(payload[offset]); offset++;

        // Read generation and version (backwards-compatible: default 0 if missing)
        uint64_t gossip_gen = 0, gossip_ver = 0;
        if (offset + sizeof(uint64_t) <= payload.size()) {
            memcpy(&gossip_gen, payload.data() + offset, sizeof(uint64_t));
            offset += sizeof(uint64_t);
        }
        if (offset + sizeof(uint64_t) <= payload.size()) {
            memcpy(&gossip_ver, payload.data() + offset, sizeof(uint64_t));
            offset += sizeof(uint64_t);
        }

        if (id == self_node.id) continue;

        NodeID gossip_node{id, ip, port, gossip_gen};
        auto it = cluster_state.find(gossip_node);
        auto gossip_status = static_cast<NodeStatus>(status_byte);

        // Stale generation — skip entirely
        if (it != cluster_state.end() && gossip_gen < it->first.generation)
            continue;

        // Version check only applies for same generation or unknown nodes
        // A higher generation always wins (covers restarted nodes with reset version)
        if (it == cluster_state.end() || gossip_gen == it->first.generation) {
            auto vit = node_versions.find(id);
            uint64_t local_ver = vit != node_versions.end() ? vit->second : 0;
            if (gossip_ver < local_ver) continue;
        }

        if (it == cluster_state.end()) {
            // Clean up any temp-ID entry for this address
            resolve_node_address(ip, port, id);
            // Unknown node — add if ALIVE or SUSPECT
            if (gossip_status == NodeStatus::ALIVE || gossip_status == NodeStatus::SUSPECT) {
                add_node(gossip_node);
                node_versions[id] = gossip_ver;
            }
        } else if (gossip_gen > it->first.generation) {
            // Higher generation wins unconditionally
            cluster_state[gossip_node] = gossip_status;
            if (gossip_status != NodeStatus::DEAD && gossip_status != NodeStatus::LEFT)
                update_last_seen(gossip_node);
            node_versions[id] = gossip_ver;
        } else if (it->second == NodeStatus::ALIVE) {
            if (gossip_status == NodeStatus::SUSPECT) {
                mark_suspect(gossip_node);
                update_last_seen(gossip_node);
            } else if (gossip_status == NodeStatus::DEAD) {
                mark_dead(gossip_node);
                build_ring();
            } else if (gossip_status == NodeStatus::LEFT) {
                remove_node(gossip_node);
                build_ring();
            } else if (gossip_status == NodeStatus::ALIVE) {
                // gossip alone should not reset the heartbeat timer
            }
            node_versions[id] = gossip_ver;
        } else if (it->second == NodeStatus::SUSPECT) {
            if (gossip_status == NodeStatus::DEAD) {
                mark_dead(gossip_node);
                build_ring();
            } else if (gossip_status == NodeStatus::LEFT) {
                remove_node(gossip_node);
                build_ring();
            } else if (gossip_status == NodeStatus::ALIVE) {
                update_last_seen(gossip_node);
            }
            node_versions[id] = gossip_ver;
        } else if (it->second == NodeStatus::DEAD && (gossip_status == NodeStatus::ALIVE || gossip_status == NodeStatus::SUSPECT)) {
            update_last_seen(gossip_node);
            node_versions[id] = gossip_ver;
        } else if (it->second == NodeStatus::DEAD && gossip_status == NodeStatus::LEFT) {
            remove_node(gossip_node);
            build_ring();
            node_versions[id] = gossip_ver;
        }
    }
}