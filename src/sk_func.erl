%%%----------------------------------------------------------------------------
%%% @author Sam Elliott <ashe@st-andrews.ac.uk>
%%% @copyright 2012 University of St Andrews (See LICENCE)
%%% @headerfile "skel.hrl"
%%%
%%% @doc This module contains the most logic of the most basic kind of skeleton
%%% - `seq'.
%%%
%%% A 'seq' instance is a wrapper for a sequential function, taking an input
%%% from its input stream, applying the function to it and sending the result
%%% on its output stream.
%%%
%%% === Example ===
%%%
%%%   ```skel:run([{seq, fun ?MODULE:p/1}, {seq, fun ?MODULE:f/1}], Input).'''
%%%
%%%     In this example, Skel is run using two sequential functions. On one
%%%     process it runs the developer-defined `p/1' on the input `Input',
%%%     sending all returned results to a second process. On this second
%%%     process, the similarly developer-defined `f/1' is run on the passed
%%%     results. This will only start once `p/1' has finished processing all
%%%     inputs and the system message `eos' sent. Results from `f/1' are sent
%%%     to the sink once they are available.
%%%
%%%
%%% @end
%%%----------------------------------------------------------------------------
-module(sk_func).

-export([
         start/2
        ,make/2
        ]).

-include("../include/skel.hrl").

-ifdef(TEST).
-compile(export_all).
-endif.

-spec make(pid(), worker_fun())  -> skel:maker_fun().
%% @doc Spawns a worker process performing the function `WorkerFun'.
%% Returns an anonymous function that takes the parent process `NextPid'
%% as an argument.
make(Monitor, WorkerFun) ->
    fun(NextPRef) ->
            sk_monitor:spawn(Monitor,
                             ?MODULE, start, [WorkerFun, NextPRef])
    end.

-spec start(worker_fun(), {pid(), reference()}) -> eos.
%% @doc Starts the worker process' task. Recursively receives the worker
%% function's input, and applies it to said function.
start(WorkerFun, NextPRef) ->
    sk_tracer:t(75, self(), {?MODULE, start}, [{next_pid, NextPRef}]),
    DataFun = sk_data:fmap(WorkerFun),
    loop(DataFun, NextPRef).

-spec loop(skel:data_fun(), {pid(), reference()}) -> eos.
%% @doc Recursively receives and applies the input to the function `DataFun'.
%% Sends the resulting data message to the process `NextPid'.
loop(DataFun, {NextPid, _} = NextPRef) ->
    receive
        {data,_,_} = DataMessage ->
            case catch(DataFun(DataMessage)) of
                {'EXIT', {_Reason, _Stack} = Err} ->
                    %% Error handling here
                    error({1, "Error when applying input to func",
                           DataMessage, Err});
                {'EXIT', Term} ->
                    %% Error handling here
                    error({2, "Error when applying input to func", Term});
                {data, _, _} = DM1 ->
                    sk_tracer:t(50, self(), NextPRef,
                                {?MODULE, data},
                                [{input, DataMessage}, {output, DM1}]),
                    NextPid ! DM1,
                    loop(DataFun, NextPRef)
                end;
        {system, eos} ->
            sk_tracer:t(75, self(), NextPid, {?MODULE, system}, [{message, eos}]),
            NextPid ! {system, eos},
            eos
    end.
