%%%----------------------------------------------------------------------------
%%% @author Sam Elliott <ashe@st-andrews.ac.uk>
%%% @copyright 2012 University of St Andrews (See LICENCE)
%%% @headerfile "skel.hrl"
%%% @doc This module is the root module of the 'Skel' library, including
%%% entry-point functions.
%%%
%%% @end
%%%----------------------------------------------------------------------------
-module(skel).

-export([
         run/2,
         do/2,
         farm/1,
         farm/2,
         farm/3,
         pipe/1,
         pipe/2,
         map/2,
         cluster/4,
         reduce/3,
         feedback/3
        ]).

-include("../include/skel.hrl").

-spec run(workflow(), input()) -> pid().
%% @doc Primary entry-point function to the Skel library. Runs a specified
%% workflow passing <tt>Input</tt> as input. Does not receive or return any
%% output from the workflow.
%%
%% <h5>Example:</h5>
%%    ```skel:run([{seq, fun ?MODULE:p/1}], Images)'''
%%
%%    Here, skel runs the function <tt>p</tt> on all items in the
%%    list <tt>Images</tt> using the Sequential Function wrapper.
%%
run(WorkFlow, Input) ->
    Monitor = sk_monitor:start(),
    sk_assembler:run(Monitor, WorkFlow, Input),
    Monitor.

receive_loop(Monitor) ->
    receive
        {'EXIT', _, normal} ->
            receive_loop(Monitor);
        {'EXIT', _, _} ->
            error("Skel: error");
        {sink_results, Results} ->
            Monitor ! {system, eos},
            Results;
        X ->
            io:format("X: ~p~n", [X])
    end.

-spec do(workflow(), list()) -> list().
%% @doc The second entry-point function to the Skel library. This function
%% <em>does</em> receive and return the results of the given workflow.
%%
%% <h5>Example:</h5>
%%    ```skel:do([{reduce, fun ?MODULE:reduce/2, fun ?MODULE:id/1}], Inputs)]'''
%%
%%      In this example, Skel uses the Reduce skeleton, where <tt>reduce</tt>
%%      and <tt>id</tt> are given as the reduction and decomposition functions
%%      respectively. The result for which is returned, and so can be printed
%%      or otherwise used.
%%
do(WorkFlow, Input) ->
    Monitor = run(WorkFlow, Input),
    receive_loop(Monitor).

-spec farm(fun()) -> wf_item().
farm(Fun) ->
    {farm, {func, Fun}}.

-spec farm(fun(), list() | integer()) -> list().
farm(Fun, NWs) when is_integer(NWs) ->
    {farm, {func, Fun}, NWs};
farm(Fun, Input) when is_list(Input) ->
    skel:do({farm, {func, Fun}}, Input).

-spec farm(fun(), non_neg_integer(), list()) -> list().
farm(Fun, N, Input) ->
    skel:do({farm, {func, Fun}, N}, Input).

-spec pipe([fun()]) -> list().
pipe(WorkflowFuns) ->
    lists:map(fun(Fun) ->
                      {func, Fun}
              end, WorkflowFuns).

-spec pipe([fun()], list()) -> list().
pipe(WorkflowFuns, Input) ->
    skel:do(lists:map(fun(Fun) ->
                              {func, Fun}
                      end, WorkflowFuns), Input).

-spec map(fun(), list()) -> list().
map(Fun, Input) when is_function(Fun, 1) ->
    skel:do({map, Fun}, Input).

cluster(Fun, Decomp, Recomp, Input) when is_function(Fun, 1) ->
    skel:do({cluster, Fun, Decomp, Recomp}, Input).

reduce(RFun, DFun, Input) when is_function(RFun, 2), is_function(DFun, 1) ->
    skel:do({reduce, RFun, DFun}, Input).

feedback(Fun, CFun, Input) when is_function(Fun, 1), is_function(CFun, 1) ->
    skel:do({feedback, Fun, CFun}, Input).
