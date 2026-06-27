#include "dispatcher.h"
#include "cluster.h"
#include "config.h"
#include "cmd.h"
#include "store.h"
#include "network.h"
#include <iostream>
#include <algorithm>
#include <chrono>
#include <sys/epoll.h>
#include <unistd.h>

#include <cstdlib>

Dispatcher dispatcher;

std::vector<str> extract_keys(const COMMAND& cmd) {
    switch (cmd.type) {
        case commandType::PING:
        case commandType::DBSIZE:
        case commandType::FLUSHDB:
        case commandType::INFO:
        case commandType::KEYS:
        case commandType::CLUSTER:
        case commandType::DASHBOARDSTATS:
            return {};
        case commandType::MGET:
        case commandType::DEL:
        case commandType::EXISTS: {
            std::vector<str> keys;
            for (size_t i = 1; i < cmd.args.size(); i++)
                keys.push_back(cmd.args[i]);
            return keys;
        }
        case commandType::MSET: {
            std::vector<str> keys;
            for (size_t i = 1; i < cmd.args.size(); i += 2)
                keys.push_back(cmd.args[i]);
            return keys;
        }
        default:
            if (cmd.args.empty()) return {};
            return {cmd.args[0]};
    }
}

static bool is_multi_key_command(commandType type) {
    return type == commandType::MGET || type == commandType::MSET ||
           type == commandType::DEL || type == commandType::EXISTS;
}

ReplicaQueueResult queue_replica(const str& raw_cmd, const std::vector<NodeID>& replicas,
                                        int client_fd, const str& deferred_response) {
    if (replicas.empty()) return ReplicaQueueResult::NO_REPLICAS;
    std::vector<uint64_t> ids;
    for (const auto& r : replicas)
        if (r.id != self_node.id)
            ids.push_back(r.id);
    if (ids.empty()) return ReplicaQueueResult::NO_REPLICAS;

    bool any_connected = false;
    for (auto id : ids) {
        auto pit = peers_by_node_id.find(id);
        // Try lazy connection if no fd exists yet
        if (pit == peers_by_node_id.end() || pit->second.fd < 0) {
            auto cit = cluster_state.find(NodeID{id, "", 0});
            if (cit != cluster_state.end())
                get_or_connect(id, cit->first.ip, cit->first.port);
            pit = peers_by_node_id.find(id);
        }
        if (pit != peers_by_node_id.end() && pit->second.fd >= 0) {
            if (!is_socket_alive(pit->second.fd)) {
                fd_to_peer_id.erase(pit->second.fd);
                epoll_ctl(epoll_fd, EPOLL_CTL_DEL, pit->second.fd, nullptr);
                close(pit->second.fd);
                pit->second.fd = -1;
                pit->second.needs_full_sync = true;
                continue;
            }
            any_connected = true;
            break;
        }
    }
    if (!any_connected) {
        if (config().strict_quorum)
            return ReplicaQueueResult::STRICT_FAIL;
        return ReplicaQueueResult::NO_REPLICAS;
    }

    str payload = raw_cmd;
    replica_queue.push_back({std::move(payload), std::move(ids), client_fd, deferred_response});
    return ReplicaQueueResult::QUEUED;
}

void embed_vclock(TOKENS& args, const Key& key) {
    ValueEntry* entry = store_get(key);
    if (!entry || entry->VecClk.empty()) return;
    args.push_back("VCLOCK");
    args.push_back(std::to_string(entry->VecClk.size()));
    for (const auto& [nid, cnt] : entry->VecClk) {
        args.push_back(std::to_string(nid));
        args.push_back(std::to_string(cnt));
    }
}

void append_vclock_aof(const str& key) {
    ValueEntry* entry = store_get(key);
    if (!entry || entry->VecClk.empty()) return;
    TOKENS t;
    t.push_back("VCLOCK");
    t.push_back(key);
    t.push_back(std::to_string(entry->VecClk.size()));
    for (const auto& [nid, cnt] : entry->VecClk) {
        t.push_back(std::to_string(nid));
        t.push_back(std::to_string(cnt));
    }
    appendAOF(RESP::array(t));
}

// Split concatenated RESP responses into individual elements
// Handles Arrays (*count\r\n... or *-1\r\n), Bulk Strings ($-1\r\n or $len\r\n...\r\n),
// Integers (:num\r\n), Simple Strings (+OK\r\n), Errors (-ERR\r\n)
std::vector<str> split_resp_responses(const str& data) {
    std::vector<str> result;
    size_t pos = 0;
    while (pos < data.size()) {
        if (data[pos] == '*') {
            // Array: skip header and extract individual elements
            size_t end = data.find("\r\n", pos);
            if (end == str::npos) break;
            int count = std::atoi(data.substr(pos + 1, end - pos - 1).c_str());
            if (count < 0) {
                // Null array (*-1\r\n)
                result.push_back(data.substr(pos, end + 2 - pos));
                pos = end + 2;
                continue;
            }
            pos = end + 2;
            for (int i = 0; i < count && pos < data.size(); i++) {
                if (data[pos] == '$') {
                    size_t e = data.find("\r\n", pos);
                    if (e == str::npos) break;
                    int len = std::atoi(data.substr(pos + 1, e - pos - 1).c_str());
                    if (len == -1) {
                        result.push_back(data.substr(pos, e + 2 - pos));
                        pos = e + 2;
                    } else {
                        size_t total = e + 2 + static_cast<size_t>(len) + 2;
                        if (total > data.size()) break;
                        result.push_back(data.substr(pos, total - pos));
                        pos = total;
                    }
                } else if (data[pos] == ':') {
                    size_t e = data.find("\r\n", pos);
                    if (e == str::npos) break;
                    result.push_back(data.substr(pos, e + 2 - pos));
                    pos = e + 2;
                } else if (data[pos] == '+') {
                    size_t e = data.find("\r\n", pos);
                    if (e == str::npos) break;
                    result.push_back(data.substr(pos, e + 2 - pos));
                    pos = e + 2;
                } else if (data[pos] == '-') {
                    size_t e = data.find("\r\n", pos);
                    if (e == str::npos) break;
                    result.push_back(data.substr(pos, e + 2 - pos));
                    pos = e + 2;
                } else break;
            }
        } else if (data[pos] == '$') {
            size_t end = data.find("\r\n", pos);
            if (end == str::npos) break;
            int len = std::atoi(data.substr(pos + 1, end - pos - 1).c_str());
            if (len == -1) {
                result.push_back(data.substr(pos, end + 2 - pos));
                pos = end + 2;
            } else {
                size_t total = end + 2 + static_cast<size_t>(len) + 2;
                if (total > data.size()) break;
                result.push_back(data.substr(pos, total - pos));
                pos = total;
            }
        } else if (data[pos] == ':') {
            size_t end = data.find("\r\n", pos);
            if (end == str::npos) break;
            result.push_back(data.substr(pos, end + 2 - pos));
            pos = end + 2;
        } else if (data[pos] == '+') {
            size_t end = data.find("\r\n", pos);
            if (end == str::npos) break;
            result.push_back(data.substr(pos, end + 2 - pos));
            pos = end + 2;
        } else if (data[pos] == '-') {
            size_t end = data.find("\r\n", pos);
            if (end == str::npos) break;
            result.push_back(data.substr(pos, end + 2 - pos));
            pos = end + 2;
        } else break;
    }
    return result;
}

static void dispatch_multi_key(Client& client, COMMAND& cmd, const std::vector<str>& keys) {
    // Group keys by owner
    std::unordered_map<uint64_t, std::vector<size_t>> owner_groups;
    for (size_t i = 0; i < keys.size(); i++) {
        NodeID owner = get_owner(keys[i]);
        owner_groups[owner.id].push_back(i);
    }

    bool has_local = owner_groups.count(self_node.id);
    int remote_targets = static_cast<int>(owner_groups.size()) - (has_local ? 1 : 0);

    // All keys on this node — execute directly
    if (remote_targets == 0) {
        str response = execute_command(cmd);
        if (is_write_command(cmd.type) && !cmd.from_replication) {
            for (auto& k : keys) embed_vclock(cmd.args, k);
            str embedded = RESP::serialize_command(cmd);
            appendAOF(embedded);
            auto qr = queue_replica(embedded, get_replicas(keys[0]),
                                    client.fd, response);
            if (qr == ReplicaQueueResult::QUEUED) return;
            if (qr == ReplicaQueueResult::STRICT_FAIL) {
                client.write_buf += RESP::error("not enough replicas reachable");
                return;
            }
        }
        client.write_buf += response;
        return;
    }

    // For MGET: per-key result slots; for MSET/DEL/EXISTS: accumulated count
    std::vector<str> parts;
    if (cmd.type == commandType::MGET) parts.resize(keys.size());
    long long accumulated = 0;

    // Execute local keys
    if (has_local) {
        const auto& local_indices = owner_groups[self_node.id];
        if (cmd.type == commandType::MGET) {
            TOKENS local_args;
            for (auto idx : local_indices)
                local_args.push_back(cmd.args[idx + 1]);
            COMMAND local_cmd = cmd;
            local_cmd.args = local_args;
            auto elements = split_resp_responses(execute_command(local_cmd));
            for (size_t ei = 0; ei < elements.size() && ei < local_indices.size(); ei++)
                parts[local_indices[ei]] = std::move(elements[ei]);
        } else if (cmd.type == commandType::MSET) {
            TOKENS local_args;
            for (auto idx : local_indices) {
                local_args.push_back(cmd.args[2 * idx + 1]);
                local_args.push_back(cmd.args[2 * idx + 2]);
            }
            COMMAND local_cmd = cmd;
            local_cmd.args = local_args;
            execute_command(local_cmd);
        } else {
            TOKENS local_args;
            for (auto idx : local_indices)
                local_args.push_back(cmd.args[idx + 1]);
            COMMAND local_cmd = cmd;
            local_cmd.args = local_args;
            str local_resp = execute_command(local_cmd);
            if (!local_resp.empty() && local_resp[0] == ':')
                accumulated += std::atol(local_resp.c_str() + 1);
        }
        // Persist locally-executed keys: embed VCLOCK, AOF, replicate
        if (is_write_command(cmd.type) && !cmd.from_replication) {
            COMMAND local_cmd = cmd;
            local_cmd.args.clear();
            for (auto idx : local_indices) {
                if (cmd.type == commandType::MSET) {
                    local_cmd.args.push_back(cmd.args[2 * idx + 1]);
                    local_cmd.args.push_back(cmd.args[2 * idx + 2]);
                } else {
                    local_cmd.args.push_back(cmd.args[idx + 1]);
                }
            }
            for (auto idx : local_indices) {
                str key;
                if (cmd.type == commandType::MSET)
                    key = cmd.args[2 * idx + 1];
                else
                    key = cmd.args[idx + 1];
                embed_vclock(local_cmd.args, key);
            }
            str embedded = RESP::serialize_command(local_cmd);
            appendAOF(embedded);
            if (!local_indices.empty()) {
                str any_key;
                if (cmd.type == commandType::MSET)
                    any_key = cmd.args[2 * local_indices[0] + 1];
                else
                    any_key = cmd.args[local_indices[0] + 1];
                queue_replica(embedded, get_replicas(any_key), -1, "");
            }
        }
    }

    static uint64_t gather_id_counter = 1;
    uint64_t gather_id = gather_id_counter++;

    // Send COMMAND frames to each remote owner
    for (const auto& [oid, indices] : owner_groups) {
        if (oid == self_node.id) continue;

        TOKENS remote_args;
        if (cmd.type == commandType::MGET) {
            for (auto idx : indices)
                remote_args.push_back(cmd.args[idx + 1]);
        } else if (cmd.type == commandType::MSET) {
            for (auto idx : indices) {
                remote_args.push_back(cmd.args[2 * idx + 1]);
                remote_args.push_back(cmd.args[2 * idx + 2]);
            }
        } else {
            for (auto idx : indices)
                remote_args.push_back(cmd.args[idx + 1]);
        }

        COMMAND remote_cmd = cmd;
        remote_cmd.args = remote_args;
        str remote_payload = RESP::serialize_command(remote_cmd);

        static uint64_t sub_id_counter = 1;
        uint64_t sub_msg_id = sub_id_counter++;

        BIN::Frame req;
        req.header = {
            .magic = BIN::MAGIC, .version = BIN::VERSION,
            .msg_type = static_cast<uint8_t>(BIN::FrameType::PROXY_REQUEST),
            .msg_id = sub_msg_id, .sender_id = self_node.id,
            .payload_len = static_cast<uint32_t>(remote_payload.size()), .checksum = 0
        };
        req.payload = std::move(remote_payload);
        auto serialized = BIN::serialize(req);

        auto pit = peers_by_node_id.find(oid);
        if (pit != peers_by_node_id.end() && pit->second.fd >= 0) {
            bool was_empty = pit->second.write_buf.empty();
            pit->second.write_buf += serialized;
            if (was_empty) {
                uint32_t flags = EPOLLIN | EPOLLOUT;
                mod_epoll(pit->second.fd, flags);
            }
        }

        gather_parent[sub_msg_id] = gather_id;
        gather_keys[sub_msg_id] = indices;
    }

    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    pending_gathers[gather_id] = {
        client.fd, cmd.type, remote_targets, 0,
        std::move(parts), accumulated, now_ms
    };
}

void Dispatcher::dispatch(Client& client, COMMAND& cmd) {
    // Reject writes if over maxmemory
    if (is_write_command(cmd.type) && !check_maxmemory_or_oom()) {
        client.write_buf += RESP::error_oom();
        return;
    }

    auto keys = extract_keys(cmd);

    if (keys.empty()) {
        client.write_buf += execute_command(cmd);
        return;
    }

    if (ring.empty()) {
        client.write_buf += execute_command(cmd);
        return;
    }

    // Multi-key scatter-gather
    if (is_multi_key_command(cmd.type) && keys.size() > 1) {
        dispatch_multi_key(client, cmd, keys);
        return;
    }

    NodeID owner = get_owner(keys[0]);
    if (owner == self_node) {
        auto response = execute_command(cmd);
        if (is_write_command(cmd.type) && !cmd.from_replication) {
            for (auto& k : keys) embed_vclock(cmd.args, k);
            str embedded = RESP::serialize_command(cmd);
            appendAOF(embedded);
            queue_replica(embedded, get_replicas(keys[0]), -1, "");
        }
        client.write_buf += response;
    } else {
        // Proxy command to the owner node
        static uint64_t proxy_msg_id = 1;
        str proxy_payload = RESP::serialize_command(cmd);
        uint64_t msg_id = proxy_msg_id++;

        BIN::Frame req;
        req.header = {
            .magic = BIN::MAGIC, .version = BIN::VERSION,
            .msg_type = static_cast<uint8_t>(BIN::FrameType::PROXY_REQUEST),
            .msg_id = msg_id, .sender_id = self_node.id,
            .payload_len = static_cast<uint32_t>(proxy_payload.size()), .checksum = 0
        };
        req.payload = std::move(proxy_payload);
        auto serialized = BIN::serialize(req);

        auto pit = peers_by_node_id.find(owner.id);
        if (pit != peers_by_node_id.end() && pit->second.fd >= 0) {
            bool was_empty = pit->second.write_buf.empty();
            pit->second.write_buf += serialized;
            if (was_empty) {
                uint32_t flags = EPOLLIN | EPOLLOUT;
                mod_epoll(pit->second.fd, flags);
            }
            auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            pending_proxy[msg_id] = {client.fd, owner.id, now_ms};
            return;
        }

        // Fallback: MOVED if owner unreachable
        uint16_t slot = crc16(keys[0]) & 0x3FFF;
        client.write_buf += RESP::error_moved(slot, owner.ip, owner.port);
    }
}
