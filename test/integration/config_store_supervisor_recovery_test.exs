defmodule Lasso.Config.ConfigStoreSupervisorRecoveryTest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag capture_log: true

  alias Lasso.BlockSync.Worker
  alias Lasso.Config.{ChainConfig, ConfigStore}
  alias Lasso.Config.ChainConfig.{Monitoring, Provider, Selection, Websocket}
  alias Lasso.Providers.Catalog

  test "published profile survives BlockSync supervisor outage and restores its worker" do
    {spec, chain_id, provider_id} = profile_spec()
    supervisor = Lasso.BlockSync.DynamicSupervisor

    on_exit(fn ->
      ensure_supervisor_started(supervisor)
      ConfigStore.remove_profile(spec.profile_id)
    end)

    assert :ok = Supervisor.terminate_child(Lasso.Supervisor, supervisor)
    assert Process.whereis(supervisor) == nil

    store = Process.whereis(ConfigStore)
    assert {:error, {:runtime_reconcile_pending, _}} = ConfigStore.inject_profile(spec)
    assert Process.whereis(ConfigStore) == store
    assert ConfigStore.status().runtime_reconcile_pending
    assert {:ok, _} = ConfigStore.get_chain(spec.profile_id, chain_id)

    assert {:ok, _pid} = Supervisor.restart_child(Lasso.Supervisor, supervisor)
    instance_id = Catalog.lookup_instance_id(spec.profile_id, chain_id, provider_id)
    assert is_binary(instance_id)

    assert_eventually(fn ->
      is_pid(GenServer.whereis(Worker.via(chain_id, instance_id))) and
        not ConfigStore.status().runtime_reconcile_pending
    end)
  end

  test "a suspended BlockSync supervisor bounds profile activation and recovers" do
    {spec, chain_id, provider_id} = profile_spec()
    supervisor = Lasso.BlockSync.DynamicSupervisor
    supervisor_pid = Process.whereis(supervisor)

    on_exit(fn ->
      :sys.resume(supervisor_pid)
      ConfigStore.remove_profile(spec.profile_id)
    end)

    assert :ok = :sys.suspend(supervisor_pid)
    store = Process.whereis(ConfigStore)
    started_at = System.monotonic_time(:millisecond)

    assert {:error, {:runtime_reconcile_pending, _}} = ConfigStore.inject_profile(spec)
    assert System.monotonic_time(:millisecond) - started_at < 4_000
    assert Process.whereis(ConfigStore) == store
    assert ConfigStore.status().runtime_reconcile_pending

    assert :ok = :sys.resume(supervisor_pid)
    instance_id = Catalog.lookup_instance_id(spec.profile_id, chain_id, provider_id)

    assert_eventually(fn ->
      is_pid(GenServer.whereis(Worker.via(chain_id, instance_id))) and
        not ConfigStore.status().runtime_reconcile_pending
    end)
  end

  test "a restarted profile-chain supervisor restores published chains without reload" do
    {spec, chain_id, _provider_id} = profile_spec()
    supervisor = Lasso.ProfileChainSupervisor

    on_exit(fn ->
      ensure_supervisor_started(supervisor)
      ConfigStore.remove_profile(spec.profile_id)
    end)

    assert :ok = ConfigStore.inject_profile(spec)
    assert Lasso.ProfileChainSupervisor.running?(spec.profile_id, chain_id)
    store = Process.whereis(ConfigStore)
    old_supervisor = Process.whereis(supervisor)
    Process.exit(old_supervisor, :kill)

    assert_eventually(fn ->
      restarted = Process.whereis(supervisor)
      is_pid(restarted) and restarted != old_supervisor
    end)

    assert_eventually(fn ->
      Lasso.ProfileChainSupervisor.running?(spec.profile_id, chain_id) and
        not ConfigStore.status().runtime_reconcile_pending
    end)

    assert Process.whereis(ConfigStore) == store
  end

  test "a stored profile update reports pending runtime work until its worker is restored" do
    {spec, chain_id, provider_id} = profile_spec()
    supervisor = Lasso.BlockSync.DynamicSupervisor

    on_exit(fn ->
      ensure_supervisor_started(supervisor)
      ConfigStore.remove_profile(spec.profile_id)
    end)

    assert :ok = ConfigStore.inject_profile(spec)
    assert :ok = Supervisor.terminate_child(Lasso.Supervisor, supervisor)

    updated_chain = %{spec.chains["runtime"] | block_time_ms: 6_000}
    updated = %{spec | chains: %{"runtime" => updated_chain}}

    assert {:error, {:runtime_reconcile_pending, _}} = ConfigStore.update_profile(updated)
    assert {:ok, stored} = ConfigStore.get_chain(spec.profile_id, chain_id)
    assert stored.block_time_ms == 6_000
    assert ConfigStore.status().runtime_reconcile_pending

    assert {:ok, _pid} = Supervisor.restart_child(Lasso.Supervisor, supervisor)
    instance_id = Catalog.lookup_instance_id(spec.profile_id, chain_id, provider_id)

    # Earlier pending operations can leave the exponential retry timer above
    # this file's default 2.5-second window during a full suite run.
    assert_eventually(
      fn ->
        is_pid(GenServer.whereis(Worker.via(chain_id, instance_id))) and
          not ConfigStore.status().runtime_reconcile_pending
      end,
      700
    )
  end

  test "batch warming publishes once, rejects malformed neighbors and repairs without republishing" do
    {first, chain, _provider} = profile_spec()
    {second, _, _} = profile_spec()
    invalid = %{second | profile_id: second.profile_id <> "-invalid", scope: :unsupported}

    on_exit(fn ->
      for spec <- [first, second], do: ConfigStore.remove_profile(spec.profile_id)
    end)

    before_generation = ConfigStore.route_generation()

    assert {:error, {:profile_publication_failed, failures}} =
             ConfigStore.ensure_profiles([first, invalid, second])

    assert Map.has_key?(failures, invalid.profile_id)
    assert ConfigStore.route_generation() == before_generation + 1
    assert Catalog.active_generation() == ConfigStore.route_generation()
    snapshot = Catalog.snapshot()
    config = :persistent_term.get(:lasso_config_store_active)
    assert :ok = ConfigStore.ensure_profiles([first, second])
    assert Catalog.snapshot() == snapshot
    assert :persistent_term.get(:lasso_config_store_active) == config
    assert :ok = Lasso.ProfileChainSupervisor.stop_profile_chain(first.profile_id, chain)
    refute Lasso.ProfileChainSupervisor.running?(first.profile_id, chain)
    assert :ok = ConfigStore.ensure_profiles([first, second])
    assert Lasso.ProfileChainSupervisor.running?(first.profile_id, chain)
    assert Catalog.snapshot() == snapshot
    assert :persistent_term.get(:lasso_config_store_active) == config
  end

  defp profile_spec do
    suffix = System.unique_integer([:positive])
    profile_id = "runtime-reconcile-#{suffix}"
    chain_id = 8_000_000 + rem(suffix, 1_000_000)
    provider_id = "provider-#{suffix}"

    chain = %ChainConfig{
      chain_id: chain_id,
      display_name: "Runtime reconciliation",
      block_time_ms: 12_000,
      providers: [
        %Provider{
          id: provider_id,
          name: "Runtime reconciliation provider",
          url: "https://reconcile-#{suffix}.example.invalid",
          subscribe_new_heads: false
        }
      ],
      monitoring: %Monitoring{probe_interval_ms: 60_000},
      selection: %Selection{},
      websocket: %Websocket{subscribe_new_heads: false}
    }

    spec = %{
      scope: :system,
      profile_id: profile_id,
      slug: profile_id,
      name: "Runtime reconciliation",
      rps_limit: 100,
      burst_limit: 200,
      unlisted: true,
      chains: %{"runtime" => chain}
    }

    {spec, chain_id, provider_id}
  end

  defp ensure_supervisor_started(supervisor) do
    case Process.whereis(supervisor) do
      nil -> Supervisor.restart_child(Lasso.Supervisor, supervisor)
      _pid -> :ok
    end
  end

  defp assert_eventually(fun, attempts \\ 700)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      assert true
    else
      Process.sleep(50)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(_fun, 0), do: flunk("condition not met in time")
end
