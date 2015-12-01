-module(sk_monitor).

-export([
         loop/1,
         start/0,
         spawn/5
        ]).

-include("../include/skel.hrl").

-spec loop(dict:dict()) -> ok.
loop(Ps) ->
    receive
        {'DOWN', Ref, process, _, normal} ->
            loop(dict:erase(Ref, Ps));
        {'DOWN', Ref, process, Pid, St} ->
            io:format(
              "~n*** sk_monitor - Process ~p (~p) down, status: ~n~p~n",
              [Pid, Ref, St]),
            error("Process Down");
        {spawn, From, M, F, A} ->
            {Pid, Ref} = spawn_monitor(M, F, A),
            From ! Pid,
            loop(dict:append(Ref, Pid, Ps));
        {system, eos} ->
            sk_tracer:t(75, self(), {?MODULE, system}, [{msg, eos}]),
            ok;
        X ->
            io:format("~n*** sk_monitor, unknown message - X: ~p~n~n", [X])
    end.

-spec start() -> pid().
start() ->
    spawn_link(?MODULE, loop, [dict:new()]).

-spec spawn(pid(), pid(), module(), atom(), list()) -> pid().
spawn(Monitor, Origin, M, F, A) ->
    Monitor ! {spawn, Origin, M, F, A},
    receive
        R when is_pid(R) ->
            R
    end.

