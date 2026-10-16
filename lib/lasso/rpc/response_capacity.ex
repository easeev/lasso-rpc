defmodule Lasso.RPC.Response.Capacity do
  @moduledoc "Ownership of retained success and authoritative error response bytes."
  alias Lasso.Core.Transport.UpstreamAdmission
  alias Lasso.JSONRPC.Error, as: JError
  alias Lasso.RPC.Response

  @spec lease(term()) :: UpstreamAdmission.Lease.t() | nil
  def lease(%{capacity_lease: lease}), do: lease
  def lease(%Response.Error{error: error}), do: lease(error)
  def lease(%{result: result}), do: lease(result)
  def lease({kind, value}) when kind in [:ok, :error], do: lease(value)
  def lease({kind, value, _elapsed}) when kind in [:ok, :error], do: lease(value)
  def lease(_), do: nil

  @spec release(term(), atom()) :: :ok | {:error, UpstreamAdmission.rejection()}
  def release(response, reason) do
    case lease(response) do
      %UpstreamAdmission.Lease{} = lease -> UpstreamAdmission.release(lease, reason)
      nil -> :ok
    end
  end

  @spec transfer(term(), pid()) :: :ok | {:error, UpstreamAdmission.rejection()}
  def transfer(response, owner) do
    case lease(response) do
      %UpstreamAdmission.Lease{} = lease -> UpstreamAdmission.transfer(lease, owner)
      nil -> :ok
    end
  end

  @doc "Delivers a retained result while its worker still owns the bytes."
  @spec deliver(term(), pid(), reference(), tuple()) :: :ok
  def deliver(result, recipient, ref, message) do
    if lease(result) do
      monitor = Process.monitor(recipient)
      send(recipient, message)

      receive do
        {:response_capacity_received, ^ref} -> :ok
        {:DOWN, ^monitor, :process, ^recipient, _} -> :ok
      end

      Process.demonitor(monitor, [:flush])
    else
      send(recipient, message)
    end

    :ok
  end

  @doc "Adopts delivered bytes before acknowledging the worker and serializing."
  @spec accept(term(), pid(), reference()) :: term()
  def accept(result, sender, ref) do
    result =
      case transfer(result, self()) do
        :ok ->
          result

        {:error, _} ->
          release(result, :handoff_failed)
          {:error, :response_handoff_failed}
      end

    send(sender, {:response_capacity_received, ref})
    result
  end

  @doc "Discards a delivery which no longer belongs to an active request."
  @spec reject(term(), pid(), reference(), atom()) :: :ok
  def reject(result, sender, ref, reason) do
    release(result, reason)
    send(sender, {:response_capacity_received, ref})
    :ok
  end

  @doc "Retain restored bytes and error data through downstream serialization and handoff."
  @spec retain(
          Response.Success.t() | JError.t(),
          UpstreamAdmission.Lease.t() | nil,
          pid(),
          non_neg_integer()
        ) ::
          {:ok, Response.Success.t() | JError.t()} | {:error, UpstreamAdmission.rejection()}
  def retain(response, nil, _owner, _wire_bytes), do: {:ok, response}

  def retain(response, lease, owner, wire_bytes) do
    # Ingress reserves twice the wire size. Success may expand when restoring a
    # client ID; errors additionally keep parsed data/provenance until consumed.
    extra =
      case response do
        %Response.Success{raw_bytes: bytes} ->
          max(2 * byte_size(bytes) - 2 * wire_bytes, 0)

        %JError{} = error ->
          # Flat heap size counts every decoded list/map container and binary
          # header conservatively, even when the term shares substructures.
          # The wire-sized allowances also cover retained binary payloads and
          # downstream serialization. Null-ID quota errors keep decoded data
          # without raw_response and need the same heap accounting.
          heap_bytes = :erts_debug.flat_size(error) * :erlang.system_info(:wordsize)

          restored_bytes =
            if is_binary(error.raw_response), do: byte_size(error.raw_response), else: 0

          max(wire_bytes + 2 * restored_bytes, 2 * wire_bytes) + heap_bytes - 2 * wire_bytes

        _ ->
          0
      end

    with :ok <- if(extra > 0, do: UpstreamAdmission.reserve_retention(lease, extra), else: :ok),
         :ok <- UpstreamAdmission.transfer(lease, owner) do
      {:ok, %{response | capacity_lease: lease}}
    end
  end

  @doc "Drop a superseded error's retained payload after its lease is released."
  @spec discard(term(), atom()) :: term()
  def discard(%JError{} = error, reason) do
    release(error, reason)
    JError.summary(error)
  end

  def discard(error, _reason), do: error
end
