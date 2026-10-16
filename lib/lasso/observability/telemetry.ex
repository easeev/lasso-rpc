defmodule Lasso.Telemetry do
  @moduledoc """
  Telemetry integration for Lasso observability.

  Provides comprehensive metrics collection, event tracking, and
  performance monitoring for the multi-provider RPC system.
  """

  use Supervisor
  require Logger
  import Telemetry.Metrics

  def start_link(arg) do
    Supervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_arg) do
    children = [
      # Telemetry poller will periodically execute the given period measurements
      {:telemetry_poller, measurements: periodic_measurements(), period: 10_000}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Attaches default telemetry handlers for logging operational events.
  Called after the supervisor tree is started.
  """
  def attach_default_handlers do
    Lasso.TelemetryLogger.attach()
  end

  @doc """
  Returns a list of Telemetry.Metrics for LiveDashboard and other metric reporters.
  """
  def metrics do
    [
      # HTTP transport I/O latency (actual network time)
      distribution("lasso.http.request.io.latency",
        event_name: [:lasso, :http, :request, :io],
        measurement: :io_ms,
        unit: {:native, :millisecond},
        description: "HTTP request I/O time (network + provider processing)",
        tags: [:provider_id, :method],
        reporter_options: [
          buckets: [10, 25, 50, 100, 250, 500, 1000, 2000, 5000]
        ]
      ),

      # WebSocket request I/O latency (send to response)
      distribution("lasso.ws.request.io.latency",
        event_name: [:lasso, :ws, :request, :io],
        measurement: :io_ms,
        unit: {:native, :millisecond},
        description: "WebSocket request I/O time (send to response)",
        tags: [:provider_id, :method],
        reporter_options: [
          buckets: [10, 25, 50, 100, 250, 500, 1000, 2000, 5000]
        ]
      ),

      # RPC request overall latency
      distribution("lasso.rpc.request.duration",
        event_name: [:lasso, :rpc, :request, :stop],
        measurement: :duration,
        unit: {:native, :millisecond},
        description: "End-to-end RPC request duration",
        tags: [:chain, :method, :provider_id, :transport, :status],
        reporter_options: [
          buckets: [10, 25, 50, 100, 250, 500, 1000, 2000, 5000, 10_000]
        ]
      ),

      # RPC request counts
      counter("lasso.rpc.diagnostic_outcome.count",
        event_name: [:lasso, :rpc, :request, :terminal],
        description: "Diagnostic routing outcomes; successful detail is sampled",
        tags: [:result, :exhaustion_cause]
      ),
      counter("lasso.routing.selection_exhausted.count",
        event_name: [:lasso, :routing, :selection_exhausted],
        description: "Exhausted routing selection phases",
        tags: [:reason]
      ),
      counter("lasso.rpc.request.count",
        event_name: [:lasso, :rpc, :request, :stop],
        description: "RPC request count",
        tags: [:chain, :method, :provider_id, :transport, :status]
      ),
      counter("lasso.rpc.routing_decision.sampled_out.count",
        event_name: [:lasso, :rpc, :routing_decision, :sampled_out],
        description: "Successful routing details omitted from the bounded live feed",
        tags: [:profile, :chain_id, :request_origin]
      ),

      # Circuit breaker state changes (individual events for each transition type)
      counter("lasso.circuit_breaker.open.count",
        event_name: [:lasso, :circuit_breaker, :open],
        description: "Circuit breaker openings",
        tags: [:instance_id, :transport, :reason]
      ),
      counter("lasso.circuit_breaker.close.count",
        event_name: [:lasso, :circuit_breaker, :close],
        description: "Circuit breaker closings",
        tags: [:instance_id, :transport, :reason]
      ),
      counter("lasso.circuit_breaker.half_open.count",
        event_name: [:lasso, :circuit_breaker, :half_open],
        description: "Circuit breaker half-open transitions",
        tags: [:instance_id, :transport, :reason]
      ),
      counter("lasso.circuit_breaker.proactive_recovery.count",
        event_name: [:lasso, :circuit_breaker, :proactive_recovery],
        description: "Circuit breaker proactive recovery attempts",
        tags: [:instance_id, :transport]
      ),
      counter("lasso.circuit_breaker.failure.count",
        event_name: [:lasso, :circuit_breaker, :failure],
        description: "Circuit breaker failures by category and state",
        tags: [:instance_id, :transport, :error_category, :circuit_state]
      ),
      counter("lasso.circuit_breaker.timeout.count",
        event_name: [:lasso, :circuit_breaker, :timeout],
        description: "Circuit breaker request timeouts",
        tags: [:instance_id, :transport]
      ),

      # WebSocket connection events
      counter("lasso.websocket.connected.count",
        event_name: [:lasso, :websocket, :connected],
        description: "WebSocket connections established",
        tags: [:provider_id, :chain]
      ),
      counter("lasso.websocket.disconnected.count",
        event_name: [:lasso, :websocket, :disconnected],
        description: "WebSocket disconnections",
        tags: [:provider_id, :chain, :unexpected]
      ),

      # WebSocket request latency (existing events)
      distribution("lasso.websocket.request.duration",
        event_name: [:lasso, :websocket, :request, :completed],
        measurement: :duration_ms,
        unit: {:native, :millisecond},
        description: "WebSocket request duration",
        tags: [:provider_id, :method, :status],
        reporter_options: [
          buckets: [10, 25, 50, 100, 250, 500, 1000, 2000, 5000]
        ]
      ),

      # Provider health metrics
      counter("lasso.provider.status.count",
        event_name: [:lasso, :provider, :status],
        description: "Provider status changes",
        tags: [:chain, :provider_id, :status]
      ),

      # Failover events (individual counters for each type)
      counter("lasso.failover.fast_fail.count",
        event_name: [:lasso, :failover, :fast_fail],
        description: "Provider failovers triggered",
        tags: [:chain, :provider_id, :transport, :error_category]
      ),
      counter("lasso.failover.circuit_open.count",
        event_name: [:lasso, :failover, :circuit_open],
        description: "Requests skipped due to open circuit",
        tags: [:chain, :provider_id, :transport]
      ),
      counter("lasso.failover.degraded_mode.count",
        event_name: [:lasso, :failover, :degraded_mode],
        description: "Degraded mode entries (trying half-open circuits)",
        tags: [:chain]
      ),
      counter("lasso.failover.degraded_success.count",
        event_name: [:lasso, :failover, :degraded_success],
        description: "Successful recoveries via degraded mode",
        tags: [:chain, :provider_id, :transport]
      ),
      counter("lasso.failover.exhaustion.count",
        event_name: [:lasso, :failover, :exhaustion],
        description: "All providers exhausted",
        tags: [:chain]
      ),

      # Cluster topology events
      counter("lasso.cluster.topology.node_connected.count",
        event_name: [:lasso, :cluster, :topology, :node_connected],
        description: "Cluster nodes connected",
        tags: [:node]
      ),
      counter("lasso.cluster.topology.node_disconnected.count",
        event_name: [:lasso, :cluster, :topology, :node_disconnected],
        description: "Cluster nodes disconnected",
        tags: [:node]
      ),

      # Dropped events in degraded mode
      counter("lasso.stream.dropped_event.count",
        event_name: [:lasso, :stream, :dropped_event],
        description: "Events dropped in degraded mode",
        tags: [:chain, :reason]
      ),
      counter("lasso.stream.continuity_resource_exhausted.count",
        event_name: [:lasso, :stream, :continuity_resource_exhausted],
        description: "Subscriptions terminated by a local continuity resource bound",
        tags: [:chain_id, :profile, :subscription_type, :reason]
      ),
      summary("lasso.stream.continuity_resource_exhausted.retained_bytes",
        event_name: [:lasso, :stream, :continuity_resource_exhausted],
        measurement: :retained_bytes,
        description: "Continuity bytes at terminal local admission"
      ),
      counter("lasso.stream.slow_consumer.count",
        event_name: [:lasso, :stream, :slow_consumer],
        description: "Downstream subscriptions terminated for bounded delivery exhaustion",
        tags: [:chain_id, :profile, :reason]
      ),
      counter("lasso.subs.reorg_repair.started.count",
        event_name: [:lasso, :subs, :reorg_repair, :started],
        description: "Connected newHeads discontinuities entering canonical repair",
        tags: [:profile, :chain_id, :provider_id, :http_provider_id]
      ),
      distribution("lasso.subs.reorg_repair.completed.duration",
        event_name: [:lasso, :subs, :reorg_repair, :completed],
        measurement: :duration_ms,
        unit: :millisecond,
        description: "Connected newHeads canonical repair duration",
        tags: [:profile, :chain_id]
      ),
      counter("lasso.subs.reorg_repair.stale_head_dropped.count",
        event_name: [:lasso, :subs, :reorg_repair, :stale_head_dropped],
        description: "Buffered fork heads suppressed after canonical HTTP reconciliation",
        tags: [:profile, :chain_id]
      ),
      counter("lasso.stream.continuity_budget.rejected.count",
        event_name: [:lasso, :stream, :continuity_budget, :rejected],
        description: "Node-wide WebSocket continuity byte admission rejections",
        tags: [:kind, :reason]
      ),
      last_value("lasso.stream.continuity_budget.used_bytes",
        event_name: [:lasso, :stream, :continuity_budget, :snapshot],
        measurement: :used_bytes,
        description: "Bytes retained by replay histories and queued downstream deliveries"
      ),
      last_value("lasso.stream.continuity_budget.owner_count",
        event_name: [:lasso, :stream, :continuity_budget, :snapshot],
        measurement: :owners,
        description: "Processes holding WebSocket continuity bytes"
      ),
      last_value("lasso.stream.continuity_budget.delivery_messages",
        event_name: [:lasso, :stream, :continuity_budget, :snapshot],
        measurement: :delivery_messages,
        description: "Admitted downstream subscription messages awaiting socket handling"
      ),
      last_value("lasso.stream.ingress.used_bytes",
        event_name: [:lasso, :stream, :ingress, :snapshot],
        measurement: :used_bytes,
        description: "Reserved internal WebSocket mailbox and processing bytes"
      ),
      last_value("lasso.stream.ingress.messages",
        event_name: [:lasso, :stream, :ingress, :snapshot],
        measurement: :messages,
        description: "Admitted internal WebSocket payload messages"
      ),
      last_value("lasso.stream.ingress.rejected",
        event_name: [:lasso, :stream, :ingress, :snapshot],
        measurement: :rejected,
        description: "Cumulative internal WebSocket admission rejections"
      ),
      last_value("lasso.stream.memory.used_bytes",
        event_name: [:lasso, :stream, :memory, :snapshot],
        measurement: :used_bytes,
        description: "Combined internal ingress, replay history, and downstream reservation bytes"
      ),
      last_value("lasso.stream.memory.limit_bytes",
        event_name: [:lasso, :stream, :memory, :snapshot],
        measurement: :limit_bytes,
        description: "Combined configured WebSocket memory reservation envelope"
      ),

      # WebSocket pending cleanup
      counter("lasso.websocket.pending_cleanup.count",
        event_name: [:lasso, :websocket, :pending_cleanup],
        description: "WebSocket pending request cleanups",
        tags: [:provider_id]
      ),
      summary("lasso.websocket.pending_cleanup.pending_count",
        event_name: [:lasso, :websocket, :pending_cleanup],
        measurement: :pending_count,
        description: "Number of pending requests cleaned up per disconnect"
      ),

      # Orphaned subscription events
      counter("lasso.upstream_subscriptions.orphaned_event.count",
        event_name: [:lasso, :upstream_subscriptions, :orphaned_event],
        description: "Orphaned subscription events received",
        tags: [:chain]
      ),

      # Dashboard cluster metrics cache
      counter("lasso_web.dashboard.cache.count",
        event_name: [:lasso_web, :dashboard, :cache],
        description: "Dashboard cluster metrics cache operations",
        tags: [:result, :profile, :chain]
      ),

      # Dashboard cluster RPC calls
      distribution("lasso_web.dashboard.cluster_rpc.duration",
        event_name: [:lasso_web, :dashboard, :cluster_rpc],
        measurement: :duration_ms,
        unit: {:native, :millisecond},
        description: "Dashboard cluster RPC call duration",
        tags: [:profile, :chain],
        reporter_options: [
          buckets: [50, 100, 250, 500, 1000, 2000, 5000]
        ]
      ),
      counter("lasso_web.dashboard.cluster_rpc.node_count",
        event_name: [:lasso_web, :dashboard, :cluster_rpc],
        measurement: :node_count,
        description: "Cluster nodes contacted for dashboard metrics"
      ),
      counter("lasso_web.dashboard.cluster_rpc.success_count",
        event_name: [:lasso_web, :dashboard, :cluster_rpc],
        measurement: :success_count,
        description: "Successful node responses for dashboard metrics"
      ),
      counter("lasso_web.dashboard.cluster_rpc.bad_count",
        event_name: [:lasso_web, :dashboard, :cluster_rpc],
        measurement: :bad_count,
        description: "Failed node responses for dashboard metrics"
      ),

      # Error classification
      counter("lasso.error_classification.classified.count",
        event_name: [:lasso, :error_classification, :classified],
        description: "Error classifications by category and path",
        tags: [:category, :classification_path, :provider_id]
      ),

      # VM metrics
      summary("vm.memory.total", unit: {:byte, :kilobyte}),
      summary("vm.total_run_queue_lengths.total"),
      summary("vm.total_run_queue_lengths.cpu"),
      summary("vm.total_run_queue_lengths.io")
    ]
  end

  defp periodic_measurements do
    [
      # Periodic system metrics
      {__MODULE__, :measure_vm_memory, []},
      {__MODULE__, :measure_run_queue, []},
      {__MODULE__, :measure_continuity_budget, []}
    ]
  end

  def measure_vm_memory do
    memory = :erlang.memory()
    total = Keyword.get(memory, :total, 0)
    :telemetry.execute([:vm, :memory], %{total: total}, %{})
  end

  def measure_run_queue do
    total = :erlang.statistics(:run_queue)
    cpu = :erlang.statistics(:run_queue)
    io = :erlang.statistics(:io)

    :telemetry.execute(
      [:vm, :total_run_queue_lengths],
      %{total: total, cpu: cpu, io: elem(io, 0)},
      %{}
    )
  end

  def measure_continuity_budget do
    case Lasso.Core.Streaming.ContinuityBudget.stats() do
      %{available?: true} = stats ->
        :telemetry.execute(
          [:lasso, :stream, :continuity_budget, :snapshot],
          Map.take(stats, [
            :used_bytes,
            :stream_bytes,
            :delivery_bytes,
            :delivery_messages,
            :peak_bytes,
            :owners
          ]),
          %{}
        )

        ingress = Lasso.Core.Streaming.Ingress.stats()
        :telemetry.execute([:lasso, :stream, :ingress, :snapshot], ingress, %{})

        :telemetry.execute(
          [:lasso, :stream, :memory, :snapshot],
          %{
            used_bytes: stats.used_bytes + ingress.used_bytes,
            limit_bytes: stats.node_limit + ingress.node_bytes
          },
          %{}
        )

      _unavailable ->
        :ok
    end
  end
end
