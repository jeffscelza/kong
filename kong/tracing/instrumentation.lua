local pdk_tracer = require "kong.pdk.tracing".new()
local time_ns    = require "kong.tools.utils".time_ns
local tablepool  = require "tablepool"
local tablex     = require "pl.tablex"
local hooks      = require "kong.hooks"

local table         = table
local pack          = table.pack
local unpack        = table.unpack

local instrument_tracer = pdk_tracer
local NOOP = function() end

local instrumentations = {}
local available_types = {}

-- db query
function instrumentations.db_query(connector)
  local f = connector.query

  local function wrap(self, sql, ...)
    local span = instrument_tracer.start_span("query", {
      attributes = {
        sql = sql,
      }
    })
    local ret = pack(f(self, sql, ...))
    span:finish()
    return unpack(ret)
  end

  connector.query = wrap
end

-- router
function instrumentations.router()
  hooks.register_hook("runloop:access:router:pre", function (ctx)
    return instrument_tracer.start_span("router")
  end)

  hooks.register_hook("runloop:access:router:post", function (span, match_t)
    if not span then
      return
    end

    span:finish()
  end)
end

-- http_request (root span)
-- we won't set the propagation headers there to avoid conflict with other tracing plugins
function instrumentations.http_request()
  local req = kong.request

  local method = req.get_method()
  local path = req.get_path()
  local span_name = method .. " " .. path

  local start_time = ngx.ctx.KONG_PROCESSING_START
      and ngx.ctx.KONG_PROCESSING_START * 100000
      or time_ns()

  -- TODO(mayo): add more attributes
  local active_span = instrument_tracer.start_span(span_name, {
    start_time_ns = start_time,
    attributes = {
      ["http.host"] = req.get_host(),
    },
  })
  instrument_tracer.set_active_span(active_span)
end

-- balancer
function instrumentations.balancer(ctx)
  local balancer_data = ctx.balancer_data
  if not balancer_data then
    return
  end

  local span
  local balancer_tries = balancer_data.tries
  local try_count = balancer_data.try_count
  for i = 1, try_count do
    local try = balancer_tries[i]
    span = instrument_tracer.start_span("balancer try #" .. i, {
      kind = 3, -- client
      start_time_ns = try.balancer_start * 100000000,
      attributes = {
        ["kong.balancer.state"] = try.state,
        ["http.status_code"] = try.code,
        ["net.peer.ip"] = try.ip,
        ["net.peer.port"] = try.port,
      }
    })

    if i < try_count then
      span:set_status(2)
    end

    if try.balancer_latency ~= nil then
      span:finish((try.balancer_start + try.balancer_latency) * 100000000)
    else
      span:finish()
    end
  end
end


local function register_plugin_hook(phase)
  return function ()
    hooks.register_hook("plugin:" .. phase .. ":before", function (plugin)
      return instrument_tracer.start_span(phase .. " phase: " .. plugin.name)
    end)
  
    hooks.register_hook("plugin:" .. phase .. ":after", function (span)
      if not span then
        return
      end
  
      span:finish()
    end)
  end
end

instrumentations.plugin_rewrite = register_plugin_hook("rewrite")
instrumentations.plugin_access = register_plugin_hook("access")
instrumentations.plugin_header_filter = register_plugin_hook("header_filter")
-- TODO(mayo): support body_filter

for k, _ in pairs(instrumentations) do
  available_types[k] = true
end
instrumentations.available_types = available_types


function instrumentations.runloop_log_before(ctx)
  -- add balancer
  instrumentations.balancer(ctx)

  local active_span = instrument_tracer.active_span()
  -- check root span type to avoid encounter error
  if active_span and type(active_span.finish) == "function" then
    active_span:finish()
  end
end

function instrumentations.runloop_log_after(ctx)
  -- Clears the span table and put back the table pool,
  -- this avoids reallocation.
  -- The span table MUST NOT be used after released.
  if type(ctx.KONG_SPANS) == "table" then
    for _, span in ipairs(ctx.KONG_SPANS) do
      tablepool.release("kong_span", span)
    end
    tablepool.release("kong_spans", ctx.KONG_SPANS)
  end
end

function instrumentations.init(config)
  local trace_types = config.opentelemetry_tracing
  local sampling_rate = config.opentelemetry_tracing_sampling_rate
  assert(type(trace_types) == "table" and #trace_types > 0)
  assert(sampling_rate >= 0 and sampling_rate <= 1)

  local enabled = trace_types[1] ~= "off"

  -- noop instrumentations
  -- TODO(mayo): support stream module
  if not enabled or ngx.config.subsystem == "stream" then
    for k, _ in pairs(available_types) do
      instrumentations[k] = NOOP
    end
  end

  if trace_types[1] ~= "all" then
    for k, _ in pairs(available_types) do
      if not tablex.find(trace_types, k) then
        instrumentations[k] = NOOP
      end
    end
  end

  if enabled then
    -- global tracer
    instrument_tracer = pdk_tracer.new("instrument", {
      sampling_rate = sampling_rate,
    })
    instrument_tracer.set_global_tracer(instrument_tracer)

    -- register hooks
    instrumentations.router()
    instrumentations.plugin_rewrite()
    instrumentations.plugin_access()
    instrumentations.plugin_header_filter()
  end
end

return instrumentations
