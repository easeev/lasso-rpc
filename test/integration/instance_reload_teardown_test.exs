defmodule Lasso.Config.InstanceReloadTeardownTest do
  use ExUnit.Case, async: false

  alias Lasso.Config.Backend.File, as: FileBackend
  alias Lasso.Config.ConfigStore
  alias Lasso.Providers.{Catalog, InstanceSupervisor}
  alias Lasso.RPC.Transport.WebSocket.Connection

  @moduletag :integration

  defmodule Upstream do
    import Plug.Conn
    def init(opts), do: opts

    def call(conn, opts) do
      {:ok, body, conn} = read_body(conn)
      request = Jason.decode!(body)
      result = if request["method"] == "eth_chainId", do: opts[:chain_id], else: "0x2a"
      send_resp(conn, 200, Jason.encode!(%{jsonrpc: "2.0", id: request["id"], result: result}))
    end
  end

  test "reload retires a stalled WS handshake without delaying healthy HTTP serving" do
    chain = 760_000_000 + rem(System.unique_integer([:positive]), 100_000_000)
    slug = "reload-stop-#{chain}"
    root = Path.join(System.tmp_dir!(), slug)
    File.mkdir_p!(root)

    File.write!(
      Path.join(root, "public.yml"),
      "---\nname: Public\nslug: public\n---\nchains: {}\n"
    )

    ref = {:reload_stop, chain}

    {:ok, _} =
      Plug.Cowboy.http(Upstream, [chain_id: "0x" <> Integer.to_string(chain, 16)],
        ref: ref,
        port: 0
      )

    url = "http://127.0.0.1:#{:ranch.get_port(ref)}"

    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, ip: {127, 0, 0, 1}])

    {:ok, ws_port} = :inet.port(listener)
    observer = self()

    start_supervised!(
      {Task,
       fn ->
         {:ok, socket} = :gen_tcp.accept(listener)
         :gen_tcp.close(listener)
         {:ok, request} = :gen_tcp.recv(socket, 0, 5_000)
         send(observer, {:handshake_started, request})
         result = :gen_tcp.recv(socket, 0, 15_000)
         send(observer, {:handshake_closed, result})
         :gen_tcp.close(socket)
       end}
    )

    %{backend_module: backend_module, backend_state: backend_state} = :sys.get_state(ConfigStore)

    env =
      Map.new(
        [:ws_client_module, :custom_origin_resolver, :http_client, :ws_startup_jitter_ms],
        &{&1, Application.fetch_env(:lasso, &1)}
      )

    on_exit(fn ->
      :gen_tcp.close(listener)

      :sys.replace_state(
        ConfigStore,
        &%{&1 | backend_module: backend_module, backend_state: backend_state}
      )

      ConfigStore.reload()

      Enum.each(env, fn
        {key, {:ok, value}} -> Application.put_env(:lasso, key, value)
        {key, :error} -> Application.delete_env(:lasso, key)
      end)

      Plug.Cowboy.shutdown(ref)
      File.rm_rf!(root)
    end)

    Application.put_env(:lasso, :ws_client_module, Lasso.RPC.Transport.WebSocket.Client)
    Application.put_env(:lasso, :http_client, Lasso.RPC.Transport.HTTP.Client.Finch)
    Application.put_env(:lasso, :ws_startup_jitter_ms, 0)

    Application.put_env(:lasso, :custom_origin_resolver, fn host ->
      if host == "stalled.ws.test", do: send(observer, {:connecting_client, self()})
      {:ok, [{127, 0, 0, 1}]}
    end)

    {:ok, file_state} =
      FileBackend.init(profiles_dir: root, legacy_config_path: root <> "/absent")

    :sys.replace_state(
      ConfigStore,
      &%{&1 | backend_module: FileBackend, backend_state: file_state}
    )

    write_profile(root, slug, chain, url, "ws://stalled.ws.test:#{ws_port}/rpc")
    assert :ok = ConfigStore.reload()
    assert_receive {:connecting_client, client}, 5_000
    assert_receive {:handshake_started, request}, 5_000
    assert request =~ "Upgrade: websocket" or String.downcase(request) =~ "upgrade: websocket"

    instance = Catalog.lookup_instance_id(slug, chain, "retiring")
    supervisor = GenServer.whereis(InstanceSupervisor.via_name(instance))
    connection = GenServer.whereis(Connection.via_instance_name(instance))
    assert is_pid(supervisor) and is_pid(connection)
    monitors = Enum.map([supervisor, connection, client], &{&1, Process.monitor(&1)})
    store = Process.whereis(ConfigStore)
    instance_root = Process.whereis(Lasso.Providers.InstanceDynamicSupervisor)
    assert_serving(slug, chain)

    write_profile(root, slug, chain, url, nil)
    started = System.monotonic_time(:millisecond)
    assert :ok = ConfigStore.reload()
    assert System.monotonic_time(:millisecond) - started < 2_000

    for {pid, monitor} <- monitors,
        do: assert_receive({:DOWN, ^monitor, :process, ^pid, _reason}, 1_000)

    assert_receive {:handshake_closed, {:error, :closed}}, 1_000
    assert GenServer.whereis(InstanceSupervisor.via_name(instance)) == nil
    assert Process.whereis(ConfigStore) == store and Process.alive?(store)

    assert Process.whereis(Lasso.Providers.InstanceDynamicSupervisor) == instance_root and
             Process.alive?(instance_root)

    assert_serving(slug, chain)
  end

  test "asynchronous handshake failures preserve network recovery classification" do
    id = "rejected-#{System.unique_integer([:positive])}"
    observer = self()
    prior = Application.fetch_env(:lasso, :ws_client_module)
    resolver = Application.fetch_env(:lasso, :custom_origin_resolver)
    jitter = Application.fetch_env(:lasso, :ws_startup_jitter_ms)
    handler = {:connect_failure, id}

    endpoint = %Lasso.RPC.Transport.WebSocket.Endpoint{
      profile: "public",
      id: id,
      name: id,
      chain_id: 77_771,
      ws_url: "ws://rejected.ws.test/rpc",
      stability_ms: 0
    }

    :ok =
      :telemetry.attach(
        handler,
        [:lasso, :websocket, :connection_failed],
        fn _, _, metadata, _ ->
          if metadata.provider_id == id, do: send(observer, {:connect_failed, metadata})
        end,
        nil
      )

    on_exit(fn ->
      :telemetry.detach(handler)

      Enum.each(
        [
          {:ws_client_module, prior},
          {:custom_origin_resolver, resolver},
          {:ws_startup_jitter_ms, jitter}
        ],
        fn
          {key, {:ok, value}} -> Application.put_env(:lasso, key, value)
          {key, :error} -> Application.delete_env(:lasso, key)
        end
      )
    end)

    Application.put_env(:lasso, :ws_client_module, Lasso.RPC.Transport.WebSocket.Client)
    Application.put_env(:lasso, :ws_startup_jitter_ms, 0)

    Application.put_env(:lasso, :custom_origin_resolver, fn _ ->
      {:error, :ssrf_blocked, "test blocked answer"}
    end)

    owner = start_supervised!({Connection, endpoint})

    assert_receive {:connect_failed,
                    %{retriable: true, will_reconnect: true, error_code: -32_004}},
                   1_000

    state = :sys.get_state(owner)
    assert state.connected == false and state.reconnect_attempts >= 1
    assert Process.alive?(owner)
    assert (Plug.Test.conn(:get, "/api/health") |> LassoWeb.Endpoint.call([])).status == 200
  end

  defp assert_serving(slug, chain) do
    conn =
      Plug.Test.conn(
        :post,
        "/rpc/profile/#{slug}/provider/stable/#{chain}",
        Jason.encode!(%{
          jsonrpc: "2.0",
          id: 1,
          method: "eth_getBalance",
          params: ["0x0000000000000000000000000000000000000001", "latest"]
        })
      )

    conn =
      conn
      |> Plug.Conn.put_req_header("content-type", "application/json")
      |> LassoWeb.Endpoint.call([])

    assert conn.status == 200
    assert Jason.decode!(conn.resp_body)["result"] == "0x2a"
    health = Plug.Test.conn(:get, "/api/health") |> LassoWeb.Endpoint.call([])
    assert health.status == 200
  end

  defp write_profile(root, slug, chain, url, ws_url) do
    retiring =
      if ws_url,
        do: "      - id: retiring\n        url: #{url}\n        ws_url: #{ws_url}\n",
        else: ""

    File.write!(Path.join(root, slug <> ".yml"), """
    ---
    name: Reload Stop
    slug: #{slug}
    ---
    chains:
      test:
        chain_id: #{chain}
        monitoring:
          background_observations: false
          chain_identity_interval_ms: 0
        websocket:
          subscribe_new_heads: false
        providers:
          - id: stable
            url: #{url}
    #{retiring}
    """)
  end
end
