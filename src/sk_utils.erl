%%%----------------------------------------------------------------------------
%%% @author Sam Elliott <ashe@st-andrews.ac.uk>
%%% @copyright 2012 University of St Andrews (See LICENCE)
%%% @headerfile "skel.hrl"
%%% @doc This module contains functions designed to start and stop worker
%%% processes, otherwise known and referred to as simply <em>workers</em>.
%%%
%%% @end
%%%----------------------------------------------------------------------------
-module(sk_utils).

-export([
         start_workers/4
        ,start_worker_hyb/4
        ,start_workers_hyb/6
        ,start_worker/3
        ,stop_workers/2
        ,cores_available/0
        ]).

-include("../include/skel.hrl").

-spec start_workers(pid(),pos_integer(), workflow(), pid()) -> [pid()].
%% @doc Starts a given number <tt>NWorkers</tt> of workers as children to the specified process <tt>NextPid</tt>. Returns a list of worker Pids.
start_workers(Monitor, NWorkers, WorkFlow, NextPid) ->
  start_workers(Monitor, NWorkers, WorkFlow, NextPid, []).

-spec start_workers_hyb(pid(), pos_integer(), pos_integer(), workflow(), workflow(), pid()) -> {[pid()],[pid()]}.
start_workers_hyb(Monitor, NCPUWorkers, NGPUWorkers, WorkFlowCPU, WorkFlowGPU, NextPid) ->
  start_workers_hyb(Monitor, NCPUWorkers, NGPUWorkers, WorkFlowCPU, WorkFlowGPU, NextPid, {[],[]}).

-spec start_workers(pid(), pos_integer(), workflow(), pid(), [pid()]) -> [pid()].
%% @doc Starts a given number <tt>NWorkers</tt> of workers as children to the
%% specified process <tt>NextPid</tt>. Returns a list of worker Pids. Inner
%% function to {@link start_workers/3}, providing storage for partial results.
start_workers(_Monitor, NWorkers,_WorkFlow,_NextPid, WorkerPids) when NWorkers < 1 ->
  WorkerPids;
start_workers(Monitor, NWorkers, WorkFlow, NextPid, WorkerPids) ->
  NewWorker = start_worker(Monitor, WorkFlow, NextPid),
  start_workers(Monitor, NWorkers-1, WorkFlow, NextPid, [NewWorker|WorkerPids]).

start_workers_hyb(_Monitor, NCPUWorkers, NGPUWorkers, _WorkFlowCPU, _WorkFlowGPU, _NextPid, Acc)
  when (NCPUWorkers < 1) and (NGPUWorkers < 1) ->
    Acc;
start_workers_hyb(Monitor, NCPUWorkers, NGPUWorkers, WorkFlowCPU, WorkFlowGPU, NextPid, {CPUWs,GPUWs})
  when NCPUWorkers < 1 ->
    NewWorker = start_worker(Monitor, WorkFlowGPU, NextPid),
    start_workers_hyb(Monitor, NCPUWorkers, NGPUWorkers-1, WorkFlowCPU, WorkFlowGPU, NextPid, {CPUWs, [NewWorker|GPUWs]});
start_workers_hyb(Monitor, NCPUWorkers, NGPUWorkers, WorkFlowCPU, WorkFlowGPU, NextPid, {CPUWs, GPUWs}) ->
    NewWorker = start_worker(Monitor, WorkFlowCPU, NextPid),
    start_workers_hyb(Monitor, NCPUWorkers-1, NGPUWorkers, WorkFlowCPU, WorkFlowGPU, NextPid, {[NewWorker|CPUWs],GPUWs}).

-spec start_worker(pid(), workflow(), pid()) -> pid().
%% @doc Provides a worker with its tasks, the workflow <tt>WorkFlow</tt>.
%% <tt>NextPid</tt> provides the output process to which the worker's results
%% are sent.
start_worker(Monitor, WorkFlow, NextPid) ->
  sk_assembler:make(Monitor, WorkFlow, NextPid).

-spec start_worker_hyb(workflow(), pid(), pos_integer(), pos_integer()) -> pid().
start_worker_hyb(WorkFlow, NextPid, NCPUWorkers, NGPUWorkers) ->
    sk_assembler:make_hyb(WorkFlow, NextPid, NCPUWorkers, NGPUWorkers).

-spec stop_workers(module(), [pid()]) -> 'eos'.
%% @doc Sends the halt command to each worker in the given list of worker
%% processes.
stop_workers(_Mod, []) ->
  eos;
stop_workers(Mod, [Worker|Rest]) ->
  sk_tracer:t(85, self(), Worker, {Mod, system}, [{msg, eos}]),
  Worker ! {system, eos},
  stop_workers(Mod, Rest).

-spec cores_available() -> non_neg_integer().
cores_available() ->
    erlang:system_info(schedulers_online).
