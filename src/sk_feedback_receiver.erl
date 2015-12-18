%%%----------------------------------------------------------------------------
%%% @author Sam Elliott <ashe@st-andrews.ac.uk>
%%% @copyright 2012 University of St Andrews (See LICENCE)
%%% @headerfile "skel.hrl"
%%%
%%% @doc This module contains Feedback skeleton filter logic.
%%%
%%% The Feedback skeleton repeatedly passes output from its inner-workflow
%%% back into said workflow until a given constraint-checking function fails.
%%%
%%% The receiver receives inputs from the previous skeleton or the feedback
%%% filter and sends them through the inner-workflow.
%%%
%%% @end
%%%----------------------------------------------------------------------------
-module(sk_feedback_receiver).

-export([
         start/5
        ]).

-include("skel.hrl").

-define(counter, sk_feedback_bicounter).

-spec start(pid(), reference(), pref(), pref(), pref()) -> 'eos'.
%% @doc Begins the receiver, taking recycled and non-recycled input and
%% passing them to a worker for application to the inner-workflow.
start(Monitor, Ref, CounterPRef, FilterPRef, WorkerPRef) ->
  sk_tracer:t(75, self(), {?MODULE, start}, [{reference, Ref},
                                             {counter_pid, CounterPRef},
                                             {filter_pid, FilterPRef},
                                             {worker_pid, WorkerPRef}]),
  setup(Ref, sk_monitor:self(Monitor), FilterPRef),
  loop(Monitor, false, CounterPRef, WorkerPRef).


% Handles messages from the previous skeleton(?) or the feedback filter.
% case 1: std. data message. Looks at first identifier in message. If not empty: decrease the counter; otherwise do nothing. Then passes the message on to the worker.
% case 2: end of stream. Receiver subscribes to the counter. So that it can find out next time it receives one if all inputs have left.
% case 3: counter. Continues looping until counter = {0,0} at which point you halt.
% case 4: system message. Pass it along.
-spec loop(pid(), boolean(), pref(), pref()) -> 'eos'.
%% @doc Inner-function for {@link start/4}; recursively receives input as data %% messages, dealing with each accordingly.
%%
%% A regular data message handling input is managed as to whether it has
%% already passed through the inner-workflow, or is a new input. A counter
%% data message may also be received, assisting with termination.
loop(Monitor, EosRecvd, CounterPRef, {WorkerPid, _} = WorkerPRef) ->
  receive
    {data,_,_} = DataMessage ->
      case sk_data:peek(DataMessage) of
        {ok, feedback} -> from_feedback(Monitor, DataMessage, CounterPRef, WorkerPRef);
        _              -> from_regular(Monitor, DataMessage, CounterPRef, WorkerPRef)
      end,
      loop(Monitor, EosRecvd, CounterPRef, WorkerPRef);
    {system, eos} ->
      ?counter:subscribe(Monitor, CounterPRef),
      loop(Monitor, true, CounterPRef, WorkerPRef);
    {system, {counter, Counters, _}} ->
      case Counters of
        {0,0} ->
          WorkerPid ! {system, eos},
          eos;
        _ ->
          loop(Monitor, EosRecvd, CounterPRef, WorkerPRef)
      end;
    {system, _} = SysMsg ->
      WorkerPid ! SysMsg,
      loop(Monitor, EosRecvd, CounterPRef, WorkerPRef)
  end.

-spec setup(reference(), pref(), pref()) -> 'ok'.
%% @doc Associates the current receiver process with the constraint-checking
%% filter process given by `FilterPid'.
setup(Ref, ReceiverPRef, {FilterPid, _} = FilterPRef) ->
  sk_tracer:t(75, self(), FilterPRef, {?MODULE, system}, [{msg, feedback_setup}, {receiver_pid, ReceiverPRef}, {reference, Ref}]),
  FilterPid ! {system, {feedback_setup, ReceiverPRef, Ref}},
  receive
    {system, {feedback_reply, FilterPRef, Ref}} ->
          ok
  end.

-spec from_feedback(pid(), data_message(), pref(), pref()) -> ok.
%% @doc Re-formats a former-output message under `DataMessage', updates the
%% counter, and passes the message to the worker process at `WorkerPid'.
from_feedback(Monitor, DataMessage, CounterPRef, {WorkerPid, _} = WorkerPRef) ->
  ?counter:cast(Monitor, CounterPRef, {incr, decr}),
  {feedback, DataMessage1} = sk_data:pop(DataMessage),
  sk_tracer:t(50, self(), WorkerPRef, {?MODULE, data}, [{output, DataMessage1}]),
  WorkerPid ! DataMessage1,
  ok.

-spec from_regular(pid(), data_message(), pref(), pref()) -> ok.
%% @doc Forwards a new input message under `DataMessage', updates the counter,
%% and passes the message onwards to the worker process at `WorkerPid'.
from_regular(Monitor, DataMessage, CounterPRef, {WorkerPid, _} = WorkerPRef) ->
  ?counter:cast(Monitor, CounterPRef, {incr, id}),
  sk_tracer:t(50, self(), WorkerPRef, {?MODULE, data}, [{output, DataMessage}]),
  WorkerPid ! DataMessage,
  ok.
