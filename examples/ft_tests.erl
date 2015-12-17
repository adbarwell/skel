-module(ft_tests).

-compile(export_all).

input() ->
    lists:seq(1, 100).

func() ->
    skel:do(fun(X) -> error(boo), X end, input()).

ord() ->
    skel:do({ord, fun(X) -> X end}, input()).

farm() ->
    skel:farm(fun(X) -> X end, input()).
