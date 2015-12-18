%%%----------------------------------------------------------------------------
%%% @author Sam Elliott <ashe@st-andrews.ac.uk>
%%% @copyright 2012 University of St Andrews (See LICENCE)
%%% @headerfile "skel.hrl"
%%%
%%% @doc This module contains Reduce skeleton decomp logic.
%%%
%%% The decomp process splits an input into many parts using a developer-
%%% defined function, then hands it on to the tree of reducer processes to be
%%% reduced.
%%%
%%% @end
%%%----------------------------------------------------------------------------
-module(sk_reduce_decomp).

-export([
         start/4
        ]).

-include("skel.hrl").

%% -type pid_pools() :: dict:dict().
-type pref_pools() :: dict:dict().

-spec start(pid(), decomp_fun(), reduce_fun(), pref()) -> eos.
%% @doc Starts the reduce process. Takes the developer-defined reduction and
%% decompostion functions, `Reduce' and `Decomp', produces a tree of processes
%% to handle reduction, and recursively reduces input.
start(Monitor, Decomp, Reduce, NextPRef) ->
  sk_tracer:t(75, self(), {?MODULE, start}, [{next_pid, NextPRef}]),
  DataDecompFun = sk_data:decomp_by(Decomp),
  DataReduceFun = sk_data:reduce_with(Reduce),
  PRefPools = dict:new(),
  NewReducer = sk_monitor:spawn(Monitor, sk_reduce_reducer, start, [DataReduceFun, NextPRef]),
  PRefPools1 = dict:store(0, [NewReducer], PRefPools),
  loop(Monitor, DataDecompFun, DataReduceFun, NextPRef, PRefPools1).

-spec loop(pid(), data_decomp_fun(), data_reduce_fun(), pref(), pref_pools()) -> eos.
%% @doc Recursively receives and reduces input. In charge of dispatching work
%% and input to reducers.
loop(Monitor, DataDecompFun, DataReduceFun, NextPRef, PRefPools) ->
  receive
    {data, _, _} = DataMessage ->
      PartitionMessages = DataDecompFun(DataMessage),
      PRefPools1 = start_reducers(Monitor, length(PartitionMessages), DataReduceFun, NextPRef, PRefPools),
      dispatch(PartitionMessages, NextPRef, PRefPools1),
      loop(Monitor, DataDecompFun, DataReduceFun, NextPRef, PRefPools1);
    {system, eos} ->
      stop_reducers(PRefPools)
  end.

-spec start_reducers(pid(), pos_integer(), data_reduce_fun(), pref(), pref_pools()) -> pref_pools().
%% @doc Recursively produces and starts reducers. Calculates the total number
%% of reducers needed based on the number of partitions `NPartitions'
%% specified. If this number has not been reached, as determined by
%% {@link top_pool/1}, create a new pool.
start_reducers(Monitor, NPartitions, DataReduceFun, NextPRef, PRefPools) ->
  TopPool = top_pool(PRefPools),
  RequiredPool = ceiling(log2(NPartitions/2)),
  if
    TopPool < RequiredPool ->
      TopPoolPRefs = dict:fetch(TopPool, PRefPools),
      NewPRefs = [sk_monitor:spawn(Monitor, sk_reduce_reducer, start, [DataReduceFun, NextPoolPRef]) || NextPoolPRef <- TopPoolPRefs, _ <- [1,2]],
      PRefPools1 = dict:store(TopPool+1, NewPRefs, PRefPools),
      start_reducers(Monitor, NPartitions, DataReduceFun, NextPRef, PRefPools1);
    true -> PRefPools
  end.

-spec dispatch([data_message(),...], pref(), pref_pools()) -> ok.
%% @doc Sends all input to reducers stored in `PidPools'.
dispatch([DataMessage] = PartitionMessages, {NextPid, _} = NextPRef, _PRefPools) when length(PartitionMessages) == 1 ->
  sk_tracer:t(75, self(), NextPRef, {?MODULE, data}, [{data_message, DataMessage}]),
  NextPid ! DataMessage,
  ok;
dispatch(PartitionMessages, _NextPRef, PRefPools) ->
  NPartitions = length(PartitionMessages),
  RequiredPool = ceiling(log2(NPartitions/2)),
  RequiredPoolPRefs = dict:fetch(RequiredPool, PRefPools),
  ReduceCount = ceiling(log2(NPartitions)),
  Ref = make_ref(),
  dispatch(Ref, ReduceCount, PartitionMessages, RequiredPoolPRefs ++ RequiredPoolPRefs).

-spec dispatch(reference(), pos_integer(), [data_message()], [pref()]) -> ok.
%% @doc Recursive worker for {@link dispatch/3}. Updates messages'
%% identifiers, and sends messages to reducers.
dispatch(_Ref, _ReduceCount, [], []) ->
  ok;
dispatch(Ref, ReduceCount, []=DataMessages, [{NextPid, _} = NextPRef|NextPRefs]) ->
  sk_tracer:t(75, self(), NextPRef, {?MODULE, system}, [{message, reduce_unit}, {ref, Ref}, {reduce_count, ReduceCount}]),
  NextPid ! {system, {reduce_unit, Ref, ReduceCount}},
  dispatch(Ref, ReduceCount, DataMessages, NextPRefs);
dispatch(Ref, ReduceCount, [DataMessage|DataMessages], [{NextPid, _} = NextPRef|NextPRefs]) ->
  DataMessage1 = sk_data:push({reduce, Ref, ReduceCount}, DataMessage),
  sk_tracer:t(50, self(), NextPRef, {?MODULE, data}, [{partition, DataMessage1}]),
  NextPid ! DataMessage1,
  dispatch(Ref, ReduceCount, DataMessages, NextPRefs).

-spec stop_reducers(pref_pools()) -> eos.
%% @doc Sends the halt command to all reducers.
stop_reducers(PRefPools) ->
  TopPoolPRefs = dict:fetch(top_pool(PRefPools), PRefPools),
  sk_utils:stop_workers(?MODULE, TopPoolPRefs ++ TopPoolPRefs).

-spec top_pool(pref_pools()) -> number().
%% @doc Finds the index of the last added PidPool.
top_pool(PRefPools) ->
  Pools = dict:fetch_keys(PRefPools),
  lists:max(Pools).

-spec log2(number()) -> number().
%% @doc Finds the binary logarithm of `X'.
log2(X) ->
  math:log(X) / math:log(2).

-spec ceiling(number()) -> integer().
%% @doc Rounds `X' up to the nearest integer.
ceiling(X) ->
  case trunc(X) of
      Y when Y < X -> Y + 1
    ; Z -> Z
  end.
