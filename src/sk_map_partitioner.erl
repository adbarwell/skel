%%%----------------------------------------------------------------------------
%%% @author Sam Elliot <ashe@st-andrews.ac.uk>
%%% @copyright 2012 University of St Andrews (See LICENCE)
%%% @headerfile "skel.hrl"
%%%
%%% @doc This module contains the simple 'map' skeleton partitioner logic.
%%%
%%% The Map skeleton is a parallel map. The skeleton applies a given function
%%% to the elements within one or more lists.
%%%
%%% The partitioner takes the input list, dispatching each partite element to
%%% a pool of worker processes. These worker processes apply the
%%% developer-defined function to each partite element.
%%%
%%% This module supports both the automatic creation of worker processes, and
%%% the ability to define the exact number to be used. With the former, the
%%% minimal number of workers are created for all inputs. This is given by the
%%% number of elements in the longest list.
%%%
%%% @end
%%%----------------------------------------------------------------------------

-module(sk_map_partitioner).

-export([
         start/4,
         start_hyb/4
        ]).

-include("skel.hrl").


-spec start(pid(), atom(), workflow() | [pref()], pref()) -> 'eos'.
%% @doc Starts the recursive partitioning of inputs.
%%
%% If the number of workers to be used is specified, a list of Pids for those
%% worker processes are received through `WorkerPids' and the second clause
%% used. Alternatively, a workflow is received as `Workflow' and the first
%% clause is used. In the case of the former, the workers have already
%% been initialised with their workflows and so the inclusion of the
%% `WorkFlow' argument is unneeded in this clause.
%%
%% The atoms `auto' and `main' are used to determine whether a list of worker
%% Pids or a workflow is received. `CombinerPid' specifies the Pid of the
%% process that will recompose the partite elements following their
%% application to the Workflow.
%%
%% @todo Wait, can't this atom be gotten rid of? The types are sufficiently different.
start(Monitor, auto, WorkFlow, CombinerPRef) ->
  sk_tracer:t(75, self(), {?MODULE, start}, [{combiner, CombinerPRef}]),
  loop(Monitor, decomp_by(), WorkFlow, CombinerPRef, []);
start(_Monitor, man, WorkerPRefs, CombinerPRef) when
      is_pid(element(1, hd(WorkerPRefs))) ->
  sk_tracer:t(75, self(), {?MODULE, start}, [{combiner, CombinerPRef}]),
  loop(decomp_by(), CombinerPRef, WorkerPRefs).

-spec start_hyb(man, [pref()], [pref()], pref()) -> 'eos'.
start_hyb(man, CPUWorkerPRefs, GPUWorkerPRefs, CombinerPRef) ->
  sk_tracer:t(75, self(), {?MODULE, start}, [{combiner, CombinerPRef}]),
  loop_hyb(decomp_by(), CombinerPRef, CPUWorkerPRefs, GPUWorkerPRefs).


-spec loop(pid(), data_decomp_fun(), workflow(), pref(), [pref()]) -> 'eos'.
%% @doc Recursively receives inputs as messages, which are decomposed, and the
%% resulting messages sent to individual workers. `loop/4' is used in place of
%% {@link loop/3} when the number of workers to be used is automatically
%% determined by the total number of partite elements of an input.
loop(Monitor, DataPartitionerFun, WorkFlow, CombinerPRef, WorkerPRefs) ->
    receive
        {data, _, _} = DataMessage ->
            PartitionMessages = DataPartitionerFun(DataMessage),
            WorkerPRefs1 = start_workers(Monitor,
                                         length(PartitionMessages),
                                         WorkFlow,
                                         CombinerPRef,
                                         WorkerPRefs),
            Ref = make_ref(),
            sk_tracer:t(60, self(), {?MODULE, data}, [{ref, Ref}, {input, DataMessage}, {partitions, PartitionMessages}]),
            dispatch(Ref,
                     length(PartitionMessages),
                     PartitionMessages,
                     WorkerPRefs1),
            loop(Monitor, DataPartitionerFun, WorkFlow, CombinerPRef, WorkerPRefs1);
        {system, eos} ->
            sk_utils:stop_workers(?MODULE, WorkerPRefs),
            eos
    end.


-spec loop(data_decomp_fun(), pref(), [pref()]) -> 'eos'.
%% @doc Recursively receives inputs as messages, which are decomposed, and the
%% resulting messages sent to individual workers. `loop/3' is used in place of
%% {@link loop/4} when the number of workers is set by the developer.
loop(DataPartitionerFun, CombinerPref, WorkerPrefs) ->
  receive
    {data, _, _} = DataMessage ->
      PartitionMessages = DataPartitionerFun(DataMessage),
      Ref = make_ref(),
      sk_tracer:t(60, self(), {?MODULE, data}, [{ref, Ref}, {input, DataMessage}, {partitions, PartitionMessages}]),
      dispatch(Ref, length(PartitionMessages), PartitionMessages, WorkerPrefs),
      loop(DataPartitionerFun, CombinerPref, WorkerPrefs);
    {system, eos} ->
      sk_utils:stop_workers(?MODULE, WorkerPrefs),
      eos
    end.

loop_hyb(DataPartitionerFun, CombinerPRef, CPUWorkerPRefs, GPUWorkerPRefs) ->
  receive
    {data, _, _} = DataMessage ->
      PartitionMessages = DataPartitionerFun(DataMessage),
      Ref = make_ref(),
      sk_tracer:t(60, self(), {?MODULE, data}, [{ref, Ref}, {input, DataMessage}, {partitions, PartitionMessages}]),
      hyb_dispatch(Ref, length(PartitionMessages), PartitionMessages, CPUWorkerPRefs, GPUWorkerPRefs),
      loop_hyb(DataPartitionerFun, CombinerPid, CPUWorkerPids, GPUWorkerPids);
    {system, eos} ->
      sk_utils:stop_workers(?MODULE, CPUWorkerPids),
      sk_utils:stop_workers(?MODULE, GPUWorkerPids),
      eos
    end.



-spec decomp_by() -> data_decomp_fun().
%% @doc Provides the decomposition function and means to split a single input
%% into many. This is based on the identity function, as the Map skeleton is
%% applied to lists.
decomp_by() ->
  fun({data, Value, Ids}) ->
    [{data, X, Ids} || X <- Value]
  end.

-spec start_workers(pid(), pos_integer(), workflow(), pref(), [pref()]) -> [pref()].
%% @doc Used when the number of workers is not set by the developer.
%%
%% Workers are started if the number needed exceeds the number we already
%% have. The total number of workers is derived from the number of partitions
%% to which `WorkFlow' will be applied, as given by `NPartitions'. This
%% includes 'recycled' workers from previous inputs. Both new and old worker
%% processes are returned so that they might be used. Worker processes are
%% represented as a list of their Pids under `WorkerPids'.
start_workers(Monitor, NPartitions, WorkFlow, CombinerPRef, WorkerPRefs) when
      NPartitions > length(WorkerPRefs) ->
    NNewWorkers = NPartitions - length(WorkerPRefs),
    NewWorkerPRefs = sk_utils:start_workers(Monitor, NNewWorkers, WorkFlow, CombinerPRef),
    NewWorkerPRefs ++ WorkerPRefs;
start_workers(_Monitor, _NPartitions, _WorkFlow, _CombinerPid, WorkerPRefs) ->
    WorkerPRefs.


-spec dispatch(reference(), pos_integer(), [data_message(),...], [pref()]) -> 'ok'.
%% @doc Partite elements of input stored in `PartitionMessages' are formatted
%% and sent to a worker from `WorkerPids'. The reference argument `Ref'
%% ensures that partite elements from different inputs are not incorrectly
%% included.
dispatch(Ref, NPartitions, PartitionMessages, WorkerPrefs) ->
  dispatch(Ref, NPartitions, 1, PartitionMessages, WorkerPrefs).

hyb_dispatch(Ref, NPartitions, PartitionMessages, CPUWorkerPids, GPUWorkerPids) ->
    hyb_dispatch(Ref, NPartitions, 1, PartitionMessages, CPUWorkerPids, GPUWorkerPids).


-spec dispatch(reference(), pos_integer(), pos_integer(), [data_message(),...], [pref()]) -> 'ok'.
%% @doc Inner-function for {@link dispatch/4}. Recursively sends each message
%% to a worker, following the addition of references to allow identification
%% and recomposition.
dispatch(_Ref,_NPartitions, _Idx, [], _) ->
    ok;
dispatch(Ref, NPartitions, Idx,
         [PartitionMessage|PartitionMessages],
         [{WPid, _} = WorkerPRef|WorkerPRefs]) ->
  PartitionMessage1 = sk_data:push({decomp, Ref, Idx, NPartitions}, PartitionMessage),
  sk_tracer:t(50, self(), WorkerPRef, {?MODULE, data}, [{partition, PartitionMessage1}]),
  WPid ! PartitionMessage1,
  dispatch(Ref, NPartitions, Idx+1, PartitionMessages, WorkerPRefs ++ [WorkerPRef]).

hyb_dispatch(_Ref,_NPartitions, _Idx, [], _, _) ->
  ok;
hyb_dispatch(Ref, NPartitions, Idx, [{DataTag,{cpu,Msg},Rest}|PartitionMessages], [{CPUWPid, _} = CPUWorkerPRef|CPUWorkerPRefs], GPUWorkerPRefs) ->
  PartitionMessageWithoutTag = {DataTag, Msg, Rest},
  PartitionMessage1 = sk_data:push({decomp, Ref, Idx, NPartitions}, PartitionMessageWithoutTag),
  sk_tracer:t(50, self(), CPUWorkerPRef, {?MODULE, data}, [{partition, PartitionMessage1}]),
  CPUWPid ! PartitionMessage1,
  hyb_dispatch(Ref, NPartitions, Idx+1, PartitionMessages, CPUWorkerPRefs ++ [CPUWorkerPRef], GPUWorkerPRefs);
hyb_dispatch(Ref, NPartitions, Idx, [{DataTag,{gpu,Msg},Rest}|PartitionMessages], CPUWorkerPRefs, [{GPUWorkerPid, _} = GPUWorkerPRef|GPUWorkerPRefs]) ->
  PartitionMessageWithoutTag = {DataTag, Msg, Rest},
  PartitionMessage1 = sk_data:push({decomp, Ref, Idx, NPartitions}, PartitionMessageWithoutTag),
  sk_tracer:t(50, self(), GPUWorkerPid, {?MODULE, data}, [{partition, PartitionMessage1}]),
  GPUWorkerPid ! PartitionMessage1,
  hyb_dispatch(Ref, NPartitions, Idx+1, PartitionMessages, CPUWorkerPRefs, GPUWorkerPRefs ++ [GPUWorkerPRef]).
