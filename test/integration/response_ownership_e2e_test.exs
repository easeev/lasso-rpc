defmodule Lasso.BackgroundResponseOwnershipE2ETest do
  use ExUnit.Case, async: false
  @moduletag :integration
  alias Lasso.Config.ConfigStore
  alias Lasso.Core.Support.CircuitBreaker
  alias Lasso.Core.Transport.UpstreamAdmission
  alias Lasso.Providers.{Catalog, InstanceState}
  alias Lasso.RPC.{RequestOptions, RequestPipeline, Response}

  defmodule Endpoint do
    import Plug.Conn
    def init(opts), do: opts

    def call(conn, opts) do
      {:ok, bytes, conn} = read_body(conn)
      request = Jason.decode!(bytes)
      send(opts[:observer], {:wire_request, conn.request_path, request["method"]})

      payload =
        cond do
          request["method"] == "eth_getCode" and conn.request_path == "/primary" ->
            %{error: %{code: -32600, message: "Opaque upstream Invalid Request"}}

          request["method"] == "resolution_heap" ->
            %{error: %{code: 3, message: "execution reverted"}}

          request["method"] in ["heap_correlated", "heap_null"] ->
            %{error: %{code: -32005, message: "Too many requests", data: List.duplicate(0, 6000)}}

          conn.request_path == "/backup" and request["method"] == "eth_getStorageAt" ->
            %{result: "0x" <> String.duplicate("0", 64)}

          conn.request_path == "/backup" ->
            %{result: "0x64"}

          request["method"] == "eth_getStorageAt" ->
            %{
              error: %{
                code: -32_000,
                message: "block range too large",
                data: %{"limit" => 10}
              }
            }

          request["method"] == "eth_getBalance" ->
            %{error: %{code: -32005, message: "Too many requests a" <> opts[:marks]}}

          true ->
            %{
              error: %{
                code: 3,
                message: "execution reverted a" <> opts[:marks],
                data: String.duplicate("x", 100_000)
              }
            }
        end

      id = if request["method"] == "heap_null", do: nil, else: request["id"]

      status =
        if request["method"] == "eth_getCode" and conn.request_path == "/primary",
          do: 400,
          else: 200

      conn
      |> put_resp_content_type("application/json")
      |> send_resp(status, Jason.encode!(Map.merge(payload, %{jsonrpc: "2.0", id: id})))
    end
  end

  setup do
    chain = 8_000_000 + System.unique_integer([:positive])
    profile = "public"
    ref = {__MODULE__, chain}

    start_supervised!(
      Plug.Cowboy.child_spec(
        scheme: :http,
        plug: {Endpoint, [observer: self(), marks: String.duplicate("\u0301", 100_000)]},
        options: [port: 0, ref: ref]
      )
    )

    port = :ranch.get_port(ref)
    old = Application.get_env(:lasso, :http_client)
    Application.put_env(:lasso, :http_client, Lasso.RPC.Transport.HTTP.Client.Finch)

    ConfigStore.register_chain_runtime(profile, chain, %{
      chain_id: chain,
      name: "Background error ownership",
      providers: [
        %{id: "primary", name: "Primary", priority: 1, url: "http://127.0.0.1:#{port}/primary"},
        %{id: "backup", name: "Backup", priority: 2, url: "http://127.0.0.1:#{port}/backup"}
      ]
    })

    Catalog.build_from_config()
    start_supervised!({Lasso.RPC.TransportRegistry, {profile, chain, %{}}})

    providers =
      Map.new(Catalog.get_profile_providers(profile, chain), &{&1.provider_id, &1.instance_id})

    for {_name, id} <- providers do
      start_supervised!(Supervisor.child_spec({CircuitBreaker, {{id, :http}, %{}}}, id: id))
      CircuitBreaker.close({id, :http})
    end

    on_exit(fn ->
      Application.put_env(:lasso, :http_client, old)
      ConfigStore.unregister_chain_runtime(profile, chain)
      Catalog.build_from_config()
      Enum.each(providers, fn {_, id} -> InstanceState.clear(id) end)
    end)

    %{chain: chain, profile: profile, providers: providers, port: port}
  end

  test "a wire range failure retains attribution and baseline failover without leaking capacity",
       c do
    baseline = UpstreamAdmission.stats().response_bytes
    owners = {Process.whereis(Lasso.Supervisor), :ets.info(:transport_channel_cache, :owner)}
    assert {supervisor, cache_owner} = owners
    assert is_pid(supervisor) and is_pid(cache_owner)

    opts = %RequestOptions{
      profile: c.profile,
      strategy: :priority,
      transport: :http,
      provider_override: "primary",
      failover_on_override: false,
      timeout_ms: 5_000
    }

    params = ["0x0000000000000000000000000000000000000001", "0x0", "latest"]

    assert {:error, error, terminal} =
             RequestPipeline.execute_via_channels(c.chain, "eth_getStorageAt", params, opts)

    assert error.resolution.constraint_claim == :block_span
    refute error.resolution.breaker_penalty?

    assert Jason.decode!(error.raw_response)["error"] == %{
             "code" => -32_000,
             "message" => "block range too large",
             "data" => %{"limit" => 10}
           }

    assert length(terminal.attempted_channels) == 1
    assert_receive {:wire_request, "/primary", "eth_getStorageAt"}
    refute_receive {:wire_request, "/backup", "eth_getStorageAt"}
    Response.Capacity.release(error, :test_terminal_consumed)
    await(fn -> UpstreamAdmission.stats().response_bytes == baseline end)

    assert {:ok, response, retried} =
             RequestPipeline.execute_via_channels(c.chain, "eth_getStorageAt", params, %{
               opts
               | provider_override: nil,
                 failover_on_override: true
             })

    assert {:ok, "0x" <> storage_word} = Response.Success.decode_result(response)
    assert storage_word == String.duplicate("0", 64)
    assert length(retried.attempted_channels) == 2
    assert_receive {:wire_request, "/primary", "eth_getStorageAt"}
    assert_receive {:wire_request, "/backup", "eth_getStorageAt"}
    Response.Capacity.release(response, :test_retry_consumed)
    await(fn -> UpstreamAdmission.stats().response_bytes == baseline end)

    assert owners ==
             {Process.whereis(Lasso.Supervisor), :ets.info(:transport_channel_cache, :owner)}
  end

  test "retry and terminal contexts bound adversarial combining messages by UTF-8 bytes", c do
    baseline = UpstreamAdmission.stats().response_bytes

    opts = %RequestOptions{
      profile: c.profile,
      strategy: :priority,
      transport: :http,
      provider_override: "primary",
      failover_on_override: false,
      timeout_ms: 5_000
    }

    assert {:error, error, terminal} =
             RequestPipeline.execute_via_channels(c.chain, "eth_call", [], opts)

    assert byte_size(error.message) > 200_000
    assert is_binary(error.raw_response)
    assert_bounded(terminal.last_attempt_error)
    assert UpstreamAdmission.stats().response_bytes > baseline
    Response.Capacity.release(error, :test_terminal_consumed)
    await(fn -> UpstreamAdmission.stats().response_bytes == baseline end)
    retry_opts = %{opts | provider_override: nil, failover_on_override: true}

    assert {:ok, response, retry} =
             RequestPipeline.execute_via_channels(c.chain, "eth_getBalance", [], retry_opts)

    assert_bounded(retry.last_attempt_error)
    Response.Capacity.release(response, :test_retry_consumed)
    await(fn -> UpstreamAdmission.stats().response_bytes == baseline end)
    assert_receive {:wire_request, "/backup", "eth_getBalance"}
  end

  test "a small authoritative error charges its resolved metadata before downstream consumption",
       c do
    baseline = UpstreamAdmission.stats().response_bytes

    assert {:error, error, ctx} =
             RequestPipeline.execute_via_channels(
               c.chain,
               "resolution_heap",
               [],
               %RequestOptions{
                 profile: c.profile,
                 strategy: :priority,
                 transport: :http,
                 timeout_ms: 5_000
               }
             )

    assert %Lasso.Core.Support.ErrorResolution{classification_path: :definitive_code} =
             error.resolution

    assert is_binary(error.raw_response)
    assert error.capacity_lease != nil
    assert length(ctx.attempted_channels) == 1

    heap_bytes = :erts_debug.flat_size(error) * :erlang.system_info(:wordsize)

    assert UpstreamAdmission.stats().response_bytes >=
             baseline + heap_bytes + 2 * byte_size(error.raw_response)

    assert :ok = Response.Capacity.release(error, :downstream_consumed)
    await(fn -> UpstreamAdmission.stats().response_bytes == baseline end)
  end

  test "decoded error containers are budgeted for correlated and null-ID upstream errors", c do
    small = Module.concat(__MODULE__, SmallHeapAdmission)
    large = Module.concat(__MODULE__, LargeHeapAdmission)

    for {admission, budget} <- [{small, 65_536}, {large, 262_144}] do
      start_supervised!(
        {UpstreamAdmission,
         name: admission,
         shards: 1,
         node_limit: 2,
         upstream_limit: 2,
         response_limit: 16_384,
         response_byte_limit: budget}
      )

      on_exit(fn -> UpstreamAdmission.stop(admission) end)
    end

    provider = %{url: "http://127.0.0.1:#{c.port}/primary"}

    for method <- ["heap_correlated", "heap_null"], admission <- [small, large] do
      context =
        Lasso.Core.Transport.AttemptProtocol.new_context(
          self(),
          make_ref(),
          System.monotonic_time(:microsecond) + 5_000_000
        )

      assert {:ok, {:raw, bytes, lease}} =
               Lasso.RPC.Transport.HTTP.Client.Finch.request(provider, method, [],
                 request_id: "lasso-heap",
                 attempt_dispatch: context,
                 admission: admission,
                 timeout: 5000
               )

      assert {:error, error} =
               Lasso.Core.Transport.UpstreamResponse.validate_unary(bytes, "lasso-heap", 19)

      heap_bytes = :erts_debug.flat_size(error.data) * :erlang.system_info(:wordsize)
      assert heap_bytes == 96_000
      assert error.raw_response == nil == (method == "heap_null")

      if admission == small do
        assert {:error, :response_byte_capacity} =
                 Response.Capacity.retain(error, lease, self(), byte_size(bytes))

        UpstreamAdmission.release(lease, :retention_refused)
      else
        assert {:ok, retained} =
                 Response.Capacity.retain(error, lease, self(), byte_size(bytes))

        assert UpstreamAdmission.stats(admission).response_bytes >=
                 heap_bytes + 2 * byte_size(bytes)

        assert UpstreamAdmission.stats(admission).response_bytes <= 262_144
        assert retained.data == List.duplicate(0, 6000)
        assert :ok = Response.Capacity.release(retained, :test_consumed)
      end

      assert UpstreamAdmission.stats(admission).response_bytes == 0
      assert UpstreamAdmission.stats(admission).node_inflight == 0
    end
  end

  test "a result is adopted before worker exit, while a killed delivery is rejected", c do
    baseline = UpstreamAdmission.stats().response_bytes
    recipient = self()

    opts = %RequestOptions{
      profile: c.profile,
      strategy: :priority,
      transport: :http,
      provider_override: "primary",
      failover_on_override: false,
      timeout_ms: 5_000
    }

    for outcome <- [:adopt, :kill] do
      ref = make_ref()

      worker =
        spawn(fn ->
          result = RequestPipeline.execute_via_channels(c.chain, "resolution_heap", [], opts)
          Response.Capacity.deliver(result, recipient, ref, {:delivery, ref, self(), result})
        end)

      monitor = Process.monitor(worker)
      assert_receive {:delivery, ^ref, ^worker, result}, 5_000
      assert UpstreamAdmission.stats().response_bytes > baseline

      if outcome == :adopt do
        assert {:error, error, _} = Response.Capacity.accept(result, worker, ref)
        assert_receive {:DOWN, ^monitor, :process, ^worker, :normal}
        assert UpstreamAdmission.stats().response_bytes > baseline
        assert :ok = Response.Capacity.release(error, :downstream_consumed)
      else
        Process.exit(worker, :kill)
        assert_receive {:DOWN, ^monitor, :process, ^worker, :killed}
        await(fn -> UpstreamAdmission.stats().response_bytes == baseline end)
        assert {:error, :response_handoff_failed} = Response.Capacity.accept(result, worker, ref)
      end

      await(fn -> UpstreamAdmission.stats().response_bytes == baseline end)
    end
  end

  test "batch workers retain authoritative errors until the batch caller consumes them", c do
    baseline = UpstreamAdmission.stats().response_bytes
    item = %{index: 0, deadline_us: System.monotonic_time(:microsecond) + 5_000_000}

    result =
      LassoWeb.RPCController.BatchExecutor.run([item], fn _, scope ->
        RequestPipeline.execute_owned(scope, c.chain, "resolution_heap", [], %RequestOptions{
          profile: c.profile,
          strategy: :priority,
          transport: :http,
          provider_override: "primary",
          failover_on_override: false,
          timeout_ms: 5_000
        })
      end)

    assert [{_, {:error, error, _}}] = result.items
    assert UpstreamAdmission.stats().response_bytes > baseline
    assert :ok = Response.Capacity.release(error, :batch_consumed)
    await(fn -> UpstreamAdmission.stats().response_bytes == baseline end)
  end

  defp assert_bounded(error) do
    assert byte_size(error.message) <= 1_000
    assert String.valid?(error.message)
    assert :binary.referenced_byte_size(error.message) == byte_size(error.message)
    assert error.data == nil
    assert error.raw_response == nil
    assert error.capacity_lease == nil
  end

  defp await(fun, left \\ 1_000)
  defp await(_fun, 0), do: flunk("background ownership did not converge")

  defp await(fun, left) do
    if fun.(),
      do: :ok,
      else:
        (
          Process.sleep(5)
          await(fun, left - 1)
        )
  end
end
