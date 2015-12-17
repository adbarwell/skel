%%%----------------------------------------------------------------------------
%%% @author Sam Elliott <ashe@st-andrews.ac.uk>
%%% @copyright 2012 University of St Andrews (See LICENCE)
%%% @headerfile "skel.hrl"
%%%
%%% @doc This module contains the initialization logic of a Farm skeleton.
%%%
%%% A task farm has the most basic kind of stream parallelism - inputs are
%%% sent to one of `n' replicas of the inner skeleton for processing.
%%%
%%% === Example ===
%%%
%%% 	```skel:run([{farm, [{seq, fun ?MODULE:p1/1}], 10}], Input)'''
%%%
%%% 	In this simple example, we produce a farm with ten workers to run the
%%% sequential, developer-defined function `p/1' using the list of inputs
%%% `Input'.
%%%
%%% @end
%%%----------------------------------------------------------------------------
-module(sk_farm).

-export([
         make/3,
         make_hyb/5
        ]).

-include("../include/skel.hrl").

-spec make(pid(), pos_integer(), workflow()) -> maker_fun().
%% @doc Initialises a Farm skeleton given the number of workers and their
%% inner-workflows, respectively.
make(Monitor, NWorkers, WorkFlow) ->
    fun(NextPRef) ->
            CollectorPRef =
                sk_monitor:spawn(Monitor,
                                 sk_farm_collector,
                                 start, [NWorkers, NextPRef]),
            WorkerPRefs = sk_utils:start_workers(Monitor, NWorkers, WorkFlow, CollectorPRef),
    sk_monitor:spawn(Monitor, sk_farm_emitter, start, [WorkerPRefs])
  end.

-spec make_hyb(pid(), pos_integer(), pos_integer(), workflow(), workflow()) -> maker_fun().
make_hyb(Monitor, NCPUWorkers, NGPUWorkers, WorkFlowCPU, WorkFlowGPU) ->
    fun(NextPid) ->
            CollectorPid = sk_monitor:spawn(Monitor,
                                            sk_farm_collector,
                                            start,
                                            [NCPUWorkers+NGPUWorkers, NextPid]),
            WorkerPids = sk_utils:start_workers_hyb(Monitor,
                                                    NCPUWorkers,
                                                    NGPUWorkers,
                                                    WorkFlowCPU,
                                                    WorkFlowGPU,
                                                    CollectorPid),
            sk_monitor:spawn(Monitor,
                             sk_farm_emitter, start, [WorkerPids])
    end.
