-module(sk_monitor).

-export([
         loop/1,
         start/0,
         spawn/4,
         spawn/5,
         spawn_worker/4
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
        {spawn_worker, From, M, F, A} ->
            {Pid, Ref} = spawn_monitor(make_worker(M, F, A)),
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

-spec spawn(pid(), module(), atom(), list()) -> pid().
spawn(Monitor, M, F, A) ->
    Monitor ! {spawn, self(), M, F, A},
    receive
        R when is_pid(R) ->
            R
    end.

-spec spawn(pid(), pid(), module(), atom(), list()) -> pid().
spawn(Monitor, Origin, M, F, A) ->
    io:format("Origin: ~p~nself(): ~p~n", [Origin, self()]),
    Monitor ! {spawn, Origin, M, F, A},
    receive
        R when is_pid(R) ->
            R
    end.

-spec spawn_worker(pid(), module(), atom(), list()) -> pid().
spawn_worker(Monitor, M, F, A) ->
    Monitor ! {spawn_worker, self(), M, F, A},
    receive
        R when is_pid(R) ->
            R
    end.

-spec make_worker(module(), atom(), list()) -> fun().
make_worker(M, F, A) ->
    fun() ->
            case catch(apply(M, F, A)) of
                {'EXIT', {_Reason, _Stack} = Err} ->
                    error({1, Err});
                {'EXIT', Term} ->
                    error({2, Term});
                X ->
                    X
            end
    end.
