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
         pipe/2
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
  sk_assembler:run(WorkFlow, Input).

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
  run(WorkFlow, Input),
  receive
    {sink_results, Results} ->
        Results
  end.

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
