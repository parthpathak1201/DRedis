#ifndef DREDIS_DISPATCHER_H
#define DREDIS_DISPATCHER_H

#include <string>
#include <vector>

#include "network.h"
#include "parser.h"

#include <cstdint>

class Dispatcher {
public:
    void dispatch(Client& client, COMMAND& cmd);
};

extern Dispatcher dispatcher;

// Exposed helper functions used by the PROXY_REQUEST handler in network.cpp
std::vector<str> extract_keys(const COMMAND& cmd);
void embed_vclock(TOKENS& args, const std::string& key);
void append_vclock_aof(const str& key);
enum class ReplicaQueueResult {
    QUEUED = 1,
    NO_REPLICAS = 0,
    STRICT_FAIL = -1
};

ReplicaQueueResult queue_replica(const str& raw_cmd, const std::vector<NodeID>& replicas,
                                 int client_fd = -1, const str& deferred_response = "");

#endif
