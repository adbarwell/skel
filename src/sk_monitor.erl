-module(sk_monitor).

-export([
         loop/1,
         start/0,
         spawn/4,
         self/1
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
            {Pid, Ref} = PRef =  spawn_monitor(M, F, A),
            From ! PRef,
            loop(dict:append(Ref, Pid, Ps));
        {self, From} ->
            [{R, [P]}] = dict:to_list(dict:filter(fun(_, [V]) ->
                                                          V =:= From
                                                  end, Ps)),
            From ! {self, {P, R}},
            loop(Ps);
        {system, eos} ->
            sk_tracer:t(75, self(), {?MODULE, system}, [{msg, eos}]),
            ok
        %% X ->
        %%     io:format("~n*** sk_monitor, unknown message - X: ~p~n~n", [X])
    end.

-spec start() -> pid().
start() ->
    spawn_link(?MODULE, loop, [dict:new()]).

-spec spawn(pid(), module(), atom(), list()) -> {pid(), reference()}.
spawn(Monitor, M, F, A) ->
    Monitor ! {spawn, erlang:self(), M, F, A},
    receive
        {P, R} = PRef when is_pid(P), is_reference(R) ->
            PRef
        %% X ->
        %%     error({"Unexpected return value when spawning.", X})
    end.

-spec self(pid()) -> pref().
self(Monitor) ->
    Monitor ! {self, erlang:self()},
    receive
        {self, PRef} ->
            PRef
    end.
