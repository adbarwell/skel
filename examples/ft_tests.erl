-module(ft_tests).

-compile(export_all).

input() ->
    lists:seq(1, 100).

minput() ->
    [lists:seq(1, 100)].

func() ->
    skel:do(fun(X) -> error(boo), X end, input()).

ord() ->
    skel:do({ord, fun(X) -> X end}, input()).

farm() ->
    skel:farm(fun(X) -> X end, input()).

map() ->
    skel:map(fun(X) -> X end, minput()).

id(X) ->
    X.

cluster() ->
    skel:cluster(fun(X) -> X end, fun(X) -> X end, fun(X) -> X end, minput()).

reduce() ->
    skel:reduce(fun(X, Y) -> X + Y end, fun(X) -> X end, minput()).
