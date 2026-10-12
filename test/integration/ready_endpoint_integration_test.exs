defmodule LassoWeb.ReadyEndpointIntegrationTest do
  use Lasso.Test.LassoIntegrationCase

  import Phoenix.ConnTest

  alias Lasso.BlockSync.Registry, as: BlockSyncRegistry
  alias Lasso.Config.ConfigStore
  alias Lasso.Providers.{CandidateListing, Catalog}
  alias Lasso.RPC.SelectionFilters

  @endpoint LassoWeb.Endpoint
  @moduletag :integration

  test "readiness follows eligible upstreams and fresh head evidence", %{chain: chain} do
    path = "/api/ready?profile=public&chain=#{chain}"
    assert %{"status" => "healthy"} = build_conn() |> get("/api/health") |> json_response(200)

    :ok =
      ConfigStore.register_chain_runtime("public", chain, %{
        chain_id: chain,
        providers: []
      })

    :ok = Catalog.build_from_config()

    assert %{"status" => "not_ready", "checks" => [%{"reason" => "no_eligible_upstream"}]} =
             build_conn() |> get(path) |> json_response(503)

    assert Lasso.Observability.Prometheus.scrape() =~
             ~s(lasso_chain_ready{profile="public",chain="#{chain}"} 0)

    setup_providers([%{id: "ready-upstream", profile: "public", behavior: :healthy}])

    Lasso.Test.Eventually.assert_eventually(fn ->
      CandidateListing.list_candidates(
        "public",
        chain,
        SelectionFilters.new(protocol: :http, exclude_rate_limited: true)
      ) != []
    end)

    :ok = BlockSyncRegistry.clear_chain(chain)

    assert %{"status" => "not_ready", "checks" => [%{"reason" => "stale_or_missing_head"}]} =
             build_conn() |> get(path) |> json_response(503)

    instance_id = Catalog.lookup_instance_id("public", chain, "ready-upstream")
    assert is_binary(instance_id)
    :ok = BlockSyncRegistry.put_height(chain, instance_id, 100, :http)

    assert %{"status" => "ready", "checks" => [%{"chain_id" => ^chain, "reason" => nil}]} =
             build_conn() |> get(path) |> json_response(200)

    assert Lasso.Observability.Prometheus.scrape() =~
             ~s(lasso_chain_ready{profile="public",chain="#{chain}"} 1)

    assert Lasso.Observability.Prometheus.scrape() =~
             ~s(lasso_chain_eligible_upstreams{profile="public",chain="#{chain}"} 1)

    :ets.insert(:lasso_instance_state, {
      {:health_block_sync, instance_id},
      %{http_status: :unhealthy, last_health_check: System.system_time(:millisecond)}
    })

    assert %{"status" => "not_ready", "checks" => [%{"reason" => "no_eligible_upstream"}]} =
             build_conn() |> get(path) |> json_response(503)
  end

  test "a dead upstream on one chain leaves the replica ready and the scoped probe not ready",
       %{chain: chain} do
    setup_providers([%{id: "dead-upstream", profile: "public", behavior: :healthy}])
    instance_id = Catalog.lookup_instance_id("public", chain, "dead-upstream")
    assert is_binary(instance_id)

    :ets.insert(:lasso_instance_state, {
      {:health_block_sync, instance_id},
      %{http_status: :unhealthy, last_health_check: System.system_time(:millisecond)}
    })

    assert %{
             "status" => "ready",
             "reason" => nil,
             "checks" => %{"configuration" => true, "catalog" => true, "cluster" => true}
           } = build_conn() |> get("/api/ready") |> json_response(200)

    assert %{
             "status" => "not_ready",
             "checks" => [%{"chain_id" => ^chain, "reason" => "no_eligible_upstream"}]
           } =
             build_conn() |> get("/api/ready?profile=public&chain=#{chain}") |> json_response(503)

    assert %{"status" => "not_ready", "profile" => "public"} =
             build_conn() |> get("/api/ready?profile=public") |> json_response(503)
  end

  test "readiness rejects a stale catalog until the current configuration is published",
       %{chain: chain} do
    setup_providers([%{id: "generation-upstream", profile: "public", behavior: :healthy}])
    instance_id = Catalog.lookup_instance_id("public", chain, "generation-upstream")
    :ok = BlockSyncRegistry.put_height(chain, instance_id, 100, :http)
    scoped = "/api/ready?profile=public&chain=#{chain}"
    owners = Enum.map([ConfigStore, Catalog.Owner], &Process.whereis/1)

    assert %{"status" => "ready"} = build_conn() |> get(scoped) |> json_response(200)

    profile = "readiness-generation-#{chain}"

    on_exit(fn ->
      ConfigStore.unregister_chain_runtime(profile, chain)
      Catalog.build_from_config()
    end)

    :ok = ConfigStore.register_chain_runtime(profile, chain, %{chain_id: chain, providers: []})
    assert Catalog.active_generation() != ConfigStore.route_generation()

    assert %{"reason" => "catalog_not_published"} =
             build_conn() |> get("/api/ready") |> json_response(503)

    assert %{"checks" => [%{"reason" => "no_eligible_upstream"}]} =
             build_conn() |> get(scoped) |> json_response(503)

    assert %{"status" => "healthy"} = build_conn() |> get("/api/health") |> json_response(200)
    assert Enum.map([ConfigStore, Catalog.Owner], &Process.whereis/1) == owners

    :ok = Catalog.build_from_config()
    :ok = BlockSyncRegistry.put_height(chain, instance_id, 100, :http)
    assert %{"status" => "ready"} = build_conn() |> get("/api/ready") |> json_response(200)
    assert %{"status" => "ready"} = build_conn() |> get(scoped) |> json_response(200)

    assert %{"result" => result} =
             build_conn()
             |> post("/rpc/#{chain}", %{
               "jsonrpc" => "2.0",
               "id" => 1,
               "method" => "eth_blockNumber",
               "params" => []
             })
             |> json_response(200)

    assert is_binary(result)
    assert %{"status" => "healthy"} = build_conn() |> get("/api/health") |> json_response(200)
    assert Enum.map([ConfigStore, Catalog.Owner], &Process.whereis/1) == owners
  end

  test "unknown chain is not ready", %{chain: chain} do
    path = "/api/ready?profile=public&chain=#{chain + 1_000_000}"

    assert %{"status" => "not_ready", "reason" => "profile_or_chain_not_configured"} =
             build_conn() |> get(path) |> json_response(503)
  end
end
