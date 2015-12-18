%%%----------------------------------------------------------------------------
%%% @author Sam Elliott <ashe@st-andrews.ac.uk>
%%% @copyright 2012 University of St Andrews (See LICENCE)
%%% @headerfile "skel.hrl"
%%%
%%% @doc This module contains 'feedback' skeleton filter logic.
%%%
%%% The Feedback skeleton repeatedly passes output from its inner-workflow
%%% back into said workflow until a given constraint-checking function fails.
%%%
%%% The filter sends the inputs back to the start of the inner skeleton should
%%% they pass a given condition; forwarding them onwards otherwise.
%%%
%%% @end
%%%----------------------------------------------------------------------------
-module(sk_feedback_filter).

-export([
         start/5
        ]).

-include("skel.hrl").

-define(counter, sk_feedback_bicounter).

-spec start(pid(), filter_fun(), reference(), pref(), pref()) -> 'eos'.
%% @doc Initialises the constraint-checking filter process.
%%
%% The developer-defined function represented by `FilterFun' serves to check
%% the desired constraint. A reference under `Ref' serves to group inputs,
%% workers and administrative processes to avoid conflicts with multiple
%% feedback skeletons. The counter process, keeping track of how many inputs
%% are currently passing through both the inner-workflow and the filter
%% process, and sink Pids are provided under `CounterPid' and `NextPid'
%% respectively.
start(Monitor, FilterFun, Ref, CounterPRef, NextPRef) ->
  sk_tracer:t(75, self(), {?MODULE, start}, [{reference, Ref},
                                             {counter_pid, CounterPRef},
                                             {next_pid, NextPRef}]),
  ReceiverPRef = setup(sk_monitor:self(Monitor), Ref),
  DataFilterFun = sk_data:filter_with(FilterFun),
  loop(Monitor, DataFilterFun, CounterPRef, ReceiverPRef, NextPRef).

-spec loop(pid(), data_filter_fun(), pref(), pref(), pref()) -> 'eos'.
%% @doc Recursively receives messages from the worker process applying the
%% inner-workflow. Messages are checked by the constraint function under
%% `DataFilterFun', then passed back into the the inner-workflow or forwarded
%% onwards to the process given by `NextPid'.
loop(Monitor, DataFilterFun, {CounterPid, _} = CounterPRef, ReceiverPRef, {NextPid, _} = NextPRef) ->
  receive
    {data,_,_} = DataMessage ->
      case DataFilterFun(DataMessage) of
        true  -> feedback(Monitor, DataMessage, CounterPRef, ReceiverPRef);
        false -> forward(Monitor, DataMessage,  CounterPRef, NextPRef)
      end,
      loop(Monitor, DataFilterFun, CounterPRef, ReceiverPRef, NextPRef);
    {system, eos} ->
      sk_tracer:t(75, self(), NextPRef, {?MODULE, system}, [{msg, eos}]),
      CounterPid ! {system, eos},
      NextPid ! {system, eos},
      eos
    end.

-spec setup(pref(), reference()) -> pref().
%% @doc Acknowledges the registration request of a receiver process, as described in {@link sk_feedback_receiver}.
setup(FilterPRef, Ref) ->
  receive
    {system, {feedback_setup, {ReceiverPid, _} = ReceiverPRef, Ref}} ->
      sk_tracer:t(75, FilterPRef, ReceiverPRef, {?MODULE, system}, [{msg, feedback_reply}, {filter_pid, FilterPRef}, {reference, Ref}]),
      ReceiverPid ! {system, {feedback_reply, FilterPRef, Ref}},
          ReceiverPRef
  end.

% counter: {no. of inputs in inner skeleton, no. of inputs in feedback loop}
-spec feedback(pid(), data_message(), pref(), pref()) -> ok.
%% @doc Inputs passing the constraint-checking function are passed here for processing.
%%
%% This function updates the counter at `CounterPid', and reformats the input
%% message given by `DataMessage' for the inner-workflow. The now-reformatted
%% message is sent to the receiver process at `Pid'.
feedback(Monitor, DataMessage, CounterPRef, {Pid, _} = PRef) ->
  ?counter:cast(Monitor, CounterPRef, {decr, incr}),
  DataMessage1 = sk_data:push(feedback, DataMessage),
  sk_tracer:t(50, self(), PRef, {?MODULE, data}, [{output, DataMessage1}]),
  Pid ! DataMessage1,
  ok.

-spec forward(pid(), data_message(), pref(), pref()) -> ok.
%% @doc New inputs are passed here for processing.
%%
%% This function simply passes the message under `DataMessage' onto the sink
%% process at `Pid' as output. The counter located at `CounterPid' is updated
%% to reflect this.
forward(Monitor, DataMessage, CounterPRef, {Pid, _} = PRef) ->
  ?counter:cast(Monitor, CounterPRef, {decr, id}),
  sk_tracer:t(50, self(), PRef, {?MODULE, data}, [{output, DataMessage}]),
  Pid ! DataMessage,
  ok.
