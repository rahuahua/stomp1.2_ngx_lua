--[[
    only support stomp1.2 now
--]]

module("stomp", package.seeall)

_VERSION = "0.10"

local sub           = string.sub
local escape_uri    = ngx.escape_uri
local match         = string.match
local tcp           = ngx.socket.tcp
local len           = string.len
local find          = string.find
local gmatch        = string.gmatch
local table_insert  = table.insert
local tonumber      = tonumber
local type          = type

local class = stomp
local mt = {__index = class}

-- error type when receive frame from MQ broker
ET_ReceiveFrameTimeout      = 1
ET_ReceiveIncorrectFrame    = 2
ET_SendAckFailed            = 3

local errinfo = {}
errinfo[ET_ReceiveFrameTimeout]     = "timeout when receiving frame from MQ broker"
errinfo[ET_ReceiveIncorrectFrame]   = "incorrect frame type received from MQ broker"

function new(self)
    return setmetatable({sock = tcp(), version = 1.2}, mt)
end

function set_timeout(self, timeout)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:settimeout(timeout)
end

function connect(self, ...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:connect(...)
end

function set_keepalive(self, ...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:setkeepalive(...)
end

function close(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:close()
end

local function string_split(inputstr, sep)
    if not sep then sep = "%s" end 
    local t = {}
    for str in gmatch(inputstr, "([^"..sep.."]+)") do
        table_insert(t, str)
    end 
    return t
end

local function split_header(header, eol)
    local t = string_split(header, eol)
    local result = {}
    for _, v in ipairs(t) do
        local t1 = string_split(v, ":")
        result[t1[1]] = t1[2]
    end

    return result
end

local function _recv_frame(self, expected_type)
    local sock = self.sock
    if not sock then
        return nil, ET_NotInitialized
    end 
    
    local readframe, err = sock:receiveuntil("\0")
    local frame, err = readframe()
    if not frame then
        return nil, ET_ReceiveFrameTimeout
    end

    --ngx.log(ngx.ERR, frame)

    if not expected_type then expected_type = "RECEIPT" end

    -- parse received frame
    local eol = "\r\n"
    local eol_len = 2
    local d_eol = "\r\n\r\n"
    local d_eol_len = 4
    local index1 = find(frame, eol)
    if not index1 then
        eol = "\n"
        eol_len = 1
        d_eol = "\n\n"
        d_eol_len = 2
        index1 = find(frame, eol)
    end
    local index2 = find(frame, d_eol)
    if not index1 or not index2 then
        return nil, ET_ReceiveIncorrectFrame
    end

    -- frame type
    local frame_type = sub(frame, 1, index1 - 1)
    
    if frame_type ~= expected_type then 
        return nil, ET_ReceiveIncorrectFrame
    end

    -- header
    local header = sub(frame, index1 + eol_len, index2 - 1)
    header = split_header(header, eol)

    -- body
    local body = nil
    if frame_type == "ERROR" or frame_type == "MESSAGE" then
        local body_len = nil
        if header and header["content-length"] then
            body_len = header["content-length"]
        end
        if body_len then 
            body_len = tonumber(body_len)
        else
            body_len = len(frame) - index2 - d_eol_len
        end
        if body_len > 0 then 
            body = sub(frame, index2 + d_eol_len, index2 + d_eol_len + body_len)
        end
    end

    return {header, body}
end

local function _send_frame(self, frame)
    local sock = self.sock
    if not sock then return nil, "not initialized" end

    --ngx.log(ngx.ERR, table.concat(frame))

    local bytes, err = sock:send(frame)
    if not bytes then return nil, "failed to send frame to MQ broker, details: " .. err end

    return true
end

-- versions should be: "1.1,1.2" or "1.0" format
function stomp(self, versions, host, login, passcode, heartbeat)
    if versions then
        local t = string_split(versions, ",")
        for _, v in ipairs(t) do
            if tonumber(v) < 1.2 then
                return nil, "only support stomp1.2 or greater"
            end
        end
    else
        versions = self.version
    end

    local t = {"STOMP\naccept-version:", versions}

    if host then
        table_insert(t, "\nhost:")
        table_insert(t, host)
    end

    if login then
        table_insert(t, "\nlogin:")
        table_insert(t, login)
    end

    if passcode then
        table_insert(t, "\npasscode:")
        table_insert(t, passcode)
    end

    if heartbeat then
        table_insert(t, "\nheartbeat:" .. heartbeat)
    end
   
    table_insert(t, "\n\n\0")
    
    local ok, err = _send_frame(self, t)
    if not ok then return nil, err end

    t, err = _recv_frame(self, "CONNECTED")
    if not t then return nil, errinfo[err] end

    local version = tonumber(t[1].version)
    if not version then
        return nil, "server error: no version header"
    end
    self.version = version

    return t[1]
end

function disconnect(self, receiptid)
    if not receiptid then receiptid = 0 end

    local t = {"DISCONNECT\n", "receipt:", receiptid, "\n\n\0"}
    local ok, err = _send_frame(self, t)
    if not ok then return nil, err end

    -- may be receive MESSAGE before RECEIPT frame
    --ok, err = _recv_frame(self)
    --if not ok then return nil, errinfo[err] end

    return true
end

-- ugly code for high performance
function send(self, destination, message, receipt, user_headers)
    if not destination or not message then
        return nil, "destination & message are required"
    end

    if user_headers and type(user_headers) ~= "table" then
        return nil, "user headers should be: {\"header1:value1\", \"header2:value2\"}"
    end
    
    local t
    if receipt and user_headers then
        local len = #user_headers
        if len == 1 then
            t = {
                "SEND\ndestination:", destination,
                "\nreceipt:", receipt,
                "\n", user_headers[1],
                "\n\n",
                message,
                "\0"
                }
        elseif len == 2 then
            t = {
                "SEND\ndestination:", destination,
                "\nreceipt:", receipt,
                "\n", user_headers[1],
                "\n", user_headers[2],
                "\n\n",
                message,
                "\0"
                }
        elseif len == 3 then
            t = {
                "SEND\ndestination:", destination,
                "\nreceipt:", receipt,
                "\n", user_headers[1],
                "\n", user_headers[2],
                "\n", user_headers[3],
                "\n\n",
                message,
                "\0"
                }
        elseif len == 4 then
            t = {
                "SEND\ndestination:", destination,
                "\nreceipt:", receipt,
                "\n", user_headers[1],
                "\n", user_headers[2],
                "\n", user_headers[3],
                "\n", user_headers[4],
                "\n\n",
                message,
                "\0"
                }
        elseif len == 5 then
            t = {
                "SEND\ndestination:", destination,
                "\nreceipt:", receipt,
                "\n", user_headers[1],
                "\n", user_headers[2],
                "\n", user_headers[3],
                "\n", user_headers[4],
                "\n", user_headers[5],
                "\n\n",
                message,
                "\0"
                }
        else
            t = {
                "SEND\ndestination:", destination,
                "\nreceipt:", receipt
                 }
            for _, v in ipairs(user_headers) do
                table_insert(t, "\n")
                table_insert(t, v)
            end

            table_insert(t, "\n\n")
            table_insert(t, message)
            table_insert(t, "\0")
        end

    elseif not receipt and user_headers then
        local len = #user_headers
        if len == 1 then
            t = {
                "SEND\ndestination:", destination,
                "\n", user_headers[1],
                "\n\n",
                message,
                "\0"
                }
        elseif len == 2 then
            t = {
                "SEND\ndestination:", destination,
                "\n", user_headers[1],
                "\n", user_headers[2],
                "\n\n",
                message,
                "\0"
                }
        elseif len == 3 then
            t = {
                "SEND\ndestination:", destination,
                "\n", user_headers[1],
                "\n", user_headers[2],
                "\n", user_headers[3],
                "\n\n",
                message,
                "\0"
                }
        elseif len == 4 then
            t = {
                "SEND\ndestination:", destination,
                "\n", user_headers[1],
                "\n", user_headers[2],
                "\n", user_headers[3],
                "\n", user_headers[4],
                "\n\n",
                message,
                "\0"
                }
        elseif len == 5 then
            t = {
                "SEND\ndestination:", destination,
                "\n", user_headers[1],
                "\n", user_headers[2],
                "\n", user_headers[3],
                "\n", user_headers[4],
                "\n", user_headers[5],
                "\n\n",
                message,
                "\0"
                }

        else
            t = {
                "SEND\ndestination:", destination,
                 }
            for _, v in ipairs(user_headers) do
                table_insert(t, "\n")
                table_insert(t, v)
            end
            table_insert(t, "\n\n")
            table_insert(t, message)
            table_insert(t, "\0")
        
        end
    elseif receipt and not user_headers then
        t = {
                "SEND\ndestination:", destination,
                "\nreceipt:", receipt,
                "\n\n",
                message,
                "\0"
            }
    elseif not content_length and not user_headers then
        t = {
                "SEND\ndestination:", destination,
                "\n\n",
                message,
                "\0"
            }
    end

    local ok, err = _send_frame(self, t)
    if not ok then return nil, err end

    if receipt then
        ok, err = _recv_frame(self)
        if not ok then return nil, errinfo[err] end
    end

    return true
end

function subscribe(self, destination, id, ack, user_headers)
    if not destination then
        return nil, "destination required"
    end

    if not id then id = 0 end

    local t = {"SUBSCRIBE\ndestination:", destination, "\nid:", id, "\nreceipt:11"}
    if ack then
        table_insert(t, "\nack:")
        table_insert(t, ack)
        self.ack_type = ack
    end
    
    if user_headers then
        for _, v in ipairs(user_headers) do
             table_insert(t, "\n")
             table_insert(t, v)
        end
    end

    table_insert(t, "\n\n\0")

    local ok, err = _send_frame(self, t)
    if not ok then return nil, err end

    ok, err = _recv_frame(self)
    if not ok then return nil, errinfo[err] end

    return true
end

function unsubscribe(self, id)
    if not id then id = 0 end

    local t = {"UNSUBSCRIBE\nid:", id, "\n\n\0"}

    local ok, err = _send_frame(self, t)
    if not ok then return nil, err end

    return true
end

function ack(self, id, txid)
    if not id then return nil, "id is required" end

    local t
    if not txid then
        t = {"ACK\nid:", id, "\n\n\0"}
    else
        t = {"ACK\nid:", id, "\ntransaction:", txid, "\n\n\0"}
    end

    local ok, err = _send_frame(self, t)
    if not ok then return nil, err end

    return true
end

function nack(self, id, txid)
    if not id  then return nil, "id is required" end

    local t
    if not txid then
        t = {"NACK\nid:", id, "\n\n\0"}
    else
        t = {"NACK\nid:", id, "\ntransaction:", txid, "\n\n\0"}
    end

    local ok, err = _send_frame(self, t)
    if not ok then return nil, err end

    return true
end

function begin(self, txid)
    if not txid  then return nil, "transaction id is required" end

    local t = {"BEGIN\ntransaction:", txid, "\n\n\0"}
    local ok, err = _send_frame(self, t)
    if not ok then return nil, err end

    ok, err = _recv_frame(self)
    if not ok then return nil, errinfo[err] end

    return true
end

function commit(self, txid)
    if not txid then return nil, "transaction id is required" end

    local t = {"COMMIT\ntransaction:", txid, "\n\n\0"}
    local ok, err = _send_frame(self, t)
    if not ok then return nil, err end

    ok, err = _recv_frame(self)
    if not ok then return nil, errinfo[err] end

    return true
end

function abort(self, txid)
    if not txid then return nil, "transaction id is required" end

    local t = {"ABORT\ntransaction:", txid, "\n\n\0"}
    local ok, err = _send_frame(self, t)
    if not ok then return nil, err end

    ok, err = _recv_frame(self)
    if not ok then return nil, errinfo[err] end

    return true
end

function recv_message(self, auto_ack)
    local t, err = _recv_frame(self, "MESSAGE")
    if not t then return nil, err end

    if auto_ack and self.ack_type ~= "auto" then
        local message_id = t[1]["ack"]
        if not message_id then return nil, ET_SendAckFailed, "no ack header in message received" end
        local ok, err = ack(self, message_id)
        if not ok then return nil, ET_SendAckFailed, err end
    end
    
    return t
end

-- to prevent use of casual module global variables
getmetatable(class).__newindex = function (table, key, val)
    error('attempt to write to undeclared variable "' .. key .. '": '
            .. debug.traceback())
end
