-module(erlite).

-include_lib("eunit/include/eunit.hrl").

-define(VERSION, {0, 1, 0}).

-export([init/0]).
-export([open/1, close/1]).
-nifs([open/1, close/1]).
-on_load(init/0).

init() -> 
  erlang:load_nif(filename:join([code:priv_dir(?MODULE), ?MODULE_STRING]), ?VERSION).

-spec open(Filename::string()) -> {ok, Database::reference()} | {error, Reason::string()}.
open(_Filename) ->
  erlang:nif_error("NIF not loaded.").

close(_Db) ->
  erlang:nif_error("NIF not loaded.").


start_test() ->
  {ok, Db} = erlite:open(":memory:"),
  ok = erlite:close(Db).
