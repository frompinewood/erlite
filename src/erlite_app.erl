%%%-------------------------------------------------------------------
%% @doc erlite public API
%% @end
%%%-------------------------------------------------------------------

-module(erlite_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    erlite_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
