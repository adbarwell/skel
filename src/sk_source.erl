%%%----------------------------------------------------------------------------
%%% @author Sam Elliott <ashe@st-andrews.ac.uk>
%%% @copyright 2012 University of St Andrews (See LICENCE)
%%% @headerfile "skel.hrl"
%%% @doc This module contains the source logic.
%%%
%%% A source is a process that provides the given inputs to the first process
%%% in a skeleton workflow.
%%%
%%% Two kinds of sources are provided - a list source (the default) and
%%% a module source, that uses a callback module to deal with the data.
%%%
%%% @end
%%%----------------------------------------------------------------------------
-module(sk_source).

-export([
         make/2
        ,start/2
        ]).

-include("../include/skel.hrl").

-ifdef(TEST).
-compile(export_all).
-endif.

-callback init() ->
    {ok, State :: term()} |
    {no_inputs, State :: term()}.

-callback next_input(State :: term()) ->
    {input, NextInput :: term(), NewState :: term()} |
    {ignore, NewState :: term()} |
    {eos, NewState :: term()}.

-callback terminate(State :: term()) ->
    ok.

%% @doc Creates a new child process using Input, given the parent process
%% <tt>Pid</tt>.
-spec make(pid(), input()) -> maker_fun().
make(Monitor, Input) ->
    fun(PRef) ->
            sk_monitor:spawn(Monitor, ?MODULE, start, [Input, PRef])
    end.

%% @doc Transmits each input in <tt>Input</tt> to the process <tt>NextPid</tt>.
%% @todo add documentation for the callback loop
-spec start(input(), {pid(), reference()}) -> 'eos'.
start(Input, NextPRef) when is_list(Input) ->
    lists:foreach(fun(X) ->
                          send_input(X, NextPRef)
                  end, Input),
    send_eos(NextPRef);
start(InputMod, NextPRef) when is_atom(InputMod) ->
    io:format("InputMod: ~p~n", [InputMod]),
    case InputMod:init() of
        {ok, State} -> callback_loop(InputMod, State, NextPRef);
        {no_inputs, State}  ->
            send_eos(NextPRef),
            InputMod:terminate(State)
    end.

%% @todo doc
callback_loop(InputMod, State, NextPid) ->
  case InputMod:next_input(State) of
    {input, NextInput, NewState} ->
      send_input(NextInput, NextPid),
      callback_loop(InputMod, NewState, NextPid);
    {ignore, NewState} ->
      callback_loop(InputMod, NewState, NextPid);
    {eos, NewState} ->
      send_eos(NextPid),
      InputMod:terminate(NewState),
      eos
  end.

%% @doc <tt>Input</tt> is formatted as a data message and sent to the
%% process <tt>NextPid</tt>.
send_input(Input, {NextPid, _}) ->
  DataMessage = sk_data:pure(Input),
  sk_tracer:t(50, self(), NextPid, {?MODULE, data}, [{output, DataMessage}]),
  NextPid ! DataMessage.

send_eos({NextPid, _}) ->
  sk_tracer:t(75, self(), NextPid, {?MODULE, system}, [{msg, eos}]),
  NextPid ! {system, eos},
  eos.
