#!/usr/bin/env lua
--[[
XADD throughput benchmark using luasocket (direct TCP/RESP).
Usage:
  lua bench_xadd.lua [host] [port] [count] [pipeline]

If luasocket is unavailable, falls back to generating RESP on stdout
which can be piped into redis-cli --pipe:
  lua bench_xadd.lua | redis-cli -p 6380 --pipe
--]]

local host = arg[1] or "127.0.0.1"
local port = tonumber(arg[2]) or 6380
local count = tonumber(arg[3]) or 100000
local pipeline = tonumber(arg[4]) or 10

local function resp_bulk(s)
    return "$" .. #s .. "\r\n" .. s .. "\r\n"
end

local function xadd_cmd(key, id, field, value)
    return "*5\r\n$4\r\nXADD\r\n"
        .. resp_bulk(key)
        .. resp_bulk(id)
        .. resp_bulk(field)
        .. resp_bulk(value)
end

local ok, socket = pcall(require, "socket")

if not ok then
    -- Fallback: generate RESP to stdout for redis-cli --pipe
    local start = os.clock()
    for i = 1, count do
        io.write(xadd_cmd("bench:" .. (i % 1000), "*", "field", "value_" .. i))
    end
    local elapsed = os.clock() - start
    io.stderr:write(string.format("Generated %d RESP commands in %.2fs\n", count, elapsed))
    io.stderr:write("Pipe this output into: redis-cli -p PORT --pipe\n")
    return
end

-- Direct TCP benchmark
local c = socket.tcp()
c:settimeout(10)
local ok, err = c:connect(host, port)
if not ok then
    io.stderr:write("Failed to connect: " .. err .. "\n")
    os.exit(1)
end

local function skip_response()
    local line, err = c:receive("*l")
    if not line then return end
    if line:sub(1, 1) == "$" then
        local len = tonumber(line:sub(2))
        if len and len > 0 then
            c:receive(len)
            c:receive(2)
        end
    end
end

local start = os.clock()
local sent = 0
while sent < count do
    local batch = math.min(pipeline, count - sent)
    local buf = {}
    for i = 0, batch - 1 do
        local idx = sent + i
        buf[i + 1] = xadd_cmd("bench:" .. (idx % 1000), "*", "field", "value_" .. idx)
    end
    c:send(table.concat(buf))
    for _ = 1, batch do
        skip_response()
    end
    sent = sent + batch
end
local elapsed = os.clock() - start
c:close()

print(string.format("XADD (pipeline=%d): %.2f req/s", pipeline, count / elapsed))
