%%%----------------------------------------------------------------------------
%%% @author Sam Elliott <ashe@st-andrews.ac.uk>
%%% @copyright 2012 University of St Andrews (See LICENCE)
%%% @headerfile "skel.hrl"
%%%
%%% @doc This module takes a workflow specification, and converts it in into a
%%% set of (concurrent) running processes.
%%%
%%%
%%% @end
%%%----------------------------------------------------------------------------
-module(sk_assembler).

-export([
         make/3
        ,make_hyb/5
        ,run/3
        ]).

-include("../include/skel.hrl").

-ifdef(TEST).
-compile(export_all).
-endif.

-spec make(pid(), workflow(), pid() | module()) -> pid() .
%% @doc Function to produce a set of processes according to the given workflow
%% specification.
make(Monitor, WorkFlow, EndModule) when is_atom(EndModule) ->
    DrainPid = (sk_sink:make(Monitor, EndModule))(self()),
    make(Monitor, WorkFlow, DrainPid);
make(Monitor, WorkFlow, EndPid) when is_pid(EndPid), is_list(WorkFlow) ->
    MakeFns = [parse(Monitor, Section) || Section <- WorkFlow],
    lists:foldr(fun(MakeFn, Pid) -> MakeFn(Pid) end, EndPid, MakeFns);
make(Monitor,  WorkFlow, EndPid) when is_pid(EndPid), is_tuple(WorkFlow) ->
    (parse(Monitor, WorkFlow))(EndPid);
make(Monitor,  WorkFlow, EndPid) when is_pid(EndPid), is_function(WorkFlow) ->
    (parse(Monitor, WorkFlow))(EndPid).


-spec make_hyb(pid(), workflow(), pid(), pos_integer(), pos_integer()) -> pid().
make_hyb(Monitor, WorkFlow, EndPid, NCPUWorkers, NGPUWorkers) when is_pid(EndPid) ->
  MakeFns = [parse_hyb(Monitor, Section, NCPUWorkers, NGPUWorkers) || Section <- WorkFlow],
  lists:foldr(fun(MakeFn, Pid) -> MakeFn(Pid) end, EndPid, MakeFns).


-spec run(pid(), pid() | workflow(), input()) -> pid().
%% @doc Function to produce and start a set of processes according to the
%% given workflow specification and input.
run(Monitor, WorkFlow, Input) when is_pid(WorkFlow) ->
    Feeder = sk_source:make(Monitor, Input),
    Feeder(WorkFlow);
run(Monitor, WorkFlow, Input) when is_list(WorkFlow) ->
    DrainPid = (sk_sink:make())(self()),
    AssembledWF = make(Monitor, WorkFlow, DrainPid),
    run(Monitor, AssembledWF, Input);
run(Monitor, WorkFlow, Input) when is_tuple(WorkFlow) ->
    run(Monitor, make(Monitor, WorkFlow, (sk_sink:make(Monitor))(self())), Input);
run(Monitor, WorkFlow, Input) when is_function(WorkFlow) ->
    run(Monitor, {func, WorkFlow}, Input).


parse_hyb(Monitor, Section, NCPUWorkers, NGPUWorkers) ->
    case Section of
        {hyb_map, WorkFlowCPU, WorkFlowGPU} ->
            parse(Monitor, {hyb_map, WorkFlowCPU, WorkFlowGPU, NCPUWorkers, NGPUWorkers});
        Other -> parse(Monitor, Other)
    end.


-spec parse(pid(), wf_item()) -> maker_fun().
%% @doc Determines the course of action to be taken according to the type of
%% workflow specified. Constructs and starts specific skeleton instances.
parse(Monitor, Fun) when is_function(Fun, 1) ->
  parse(Monitor, {func, Fun});
parse(Monitor, {func, Fun}) when is_function(Fun, 1) ->
  sk_seq:make(Monitor, Fun);
parse(Monitor, {seq, Fun}) when is_function(Fun, 1) ->
  sk_seq:make(Monitor, Fun);
parse(Monitor, {pipe, WorkFlow}) ->
  sk_pipe:make(Monitor, WorkFlow);
parse(Monitor, {ord, WorkFlow}) ->
  sk_ord:make(Monitor, WorkFlow);
parse(Monitor, {farm, WorkFlow}) ->
  sk_farm:make(Monitor, sk_utils:cores_available(), WorkFlow);
parse(Monitor, {farm, WorkFlow, NWorkers}) ->
  sk_farm:make(Monitor, NWorkers, WorkFlow);
parse(Monitor, {hyb_farm, WorkFlowCPU, WorkFlowGPU, NCPUWorkers, NGPUWorkers}) ->
  sk_farm:make_hyb(Monitor, NCPUWorkers, NGPUWorkers, WorkFlowCPU, WorkFlowGPU);
parse(Monitor, {map, WorkFlow}) ->
  sk_map:make(WorkFlow);
parse(Monitor, {map, WorkFlow, NWorkers}) ->
  sk_map:make(WorkFlow, NWorkers);
parse(Monitor, {hyb_map, WorkFlowCPU, WorkFlowGPU}) ->
    sk_map:make_hyb(Monitor, WorkFlowCPU, WorkFlowGPU);
parse(Monitor, {hyb_map, WorkFlowCPU, WorkFlowGPU, NCPUWorkers, NGPUWorkers}) ->
  sk_map:make_hyb(Monitor, WorkFlowCPU, WorkFlowGPU, NCPUWorkers, NGPUWorkers);
parse(Monitor, {cluster, WorkFlow, Decomp, Recomp}) when is_function(Decomp, 1),
                                               is_function(Recomp, 1) ->
  sk_cluster:make(WorkFlow, Decomp, Recomp);
parse(Monitor, {hyb_cluster, WorkFlow, Decomp, Recomp, NCPUWorkers, NGPUWorkers}) when
      is_function(Decomp, 1), is_function(Recomp, 1) ->
    sk_cluster:make_hyb(Monitor, WorkFlow, Decomp, Recomp, NCPUWorkers, NGPUWorkers);
parse(Monitor, {hyb_cluster, WorkFlow, TimeRatio, NCPUWorkers, NGPUWorkers}) ->
    sk_cluster:make_hyb(Monitor, WorkFlow, TimeRatio, NCPUWorkers, NGPUWorkers);
parse(Monitor, {hyb_cluster, WorkFlow, TimeRatio, StructSizeFun, MakeChunkFun, RecompFun, NCPUWorkers, NGPUWorkers}) ->
    sk_cluster:make_hyb(Monitor, WorkFlow, TimeRatio, StructSizeFun, MakeChunkFun, RecompFun, NCPUWorkers, NGPUWorkers);

% parse({decomp, WorkFlow, Decomp, Recomp}) when is_function(Decomp, 1),
%                                                is_function(Recomp, 1) ->
%   sk_decomp:make(WorkFlow, Decomp, Recomp);
% parse({map, WorkFlow, Decomp, Recomp}) when is_function(Decomp, 1),
%                                             is_function(Recomp, 1) ->
%   sk_map:make(WorkFlow, Decomp, Recomp);
parse(Monitor, {reduce, Reduce, Decomp}) when is_function(Reduce, 2),
                                     is_function(Decomp, 1) ->
  sk_reduce:make(Decomp, Reduce);
parse(Monitor, {feedback, WorkFlow, Filter}) when is_function(Filter, 1) ->
  sk_feedback:make(WorkFlow, Filter).
