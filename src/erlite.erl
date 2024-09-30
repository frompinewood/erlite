-module(erlite).

-include("erlite.hrl").

-include_lib("eunit/include/eunit.hrl").

-export([open/0, init/0]).
-export([
    open/1,
    close/1,
    prepare/2,
    finalize/1,
    reset/1,
    step/1,
    bind/3,
    result/2,
    bind_count/1,
    column_count/1,
    column_name/2,
    exec/2,
    exec/3
]).
-nifs([
    open/1,
    close/1,
    prepare/2,
    finalize/1,
    reset/1,
    step/1,
    bind/3,
    result/2,
    bind_count/1,
    column_count/1,
    column_name/2
]).
-on_load(init/0).

init() ->
    erlang:load_nif(filename:join([code:priv_dir(?MODULE), ?MODULE_STRING]), ?ERLITE_VERSION).

%%% @doc
%%% Opens a Sqlite3 database in-memory.
%%% @end
-spec open() -> {ok, Database :: reference()} | {error, Reason :: string()}.
open() -> open(":memory:").

-spec open(Filename :: string()) -> {ok, Database :: reference()} | {error, Reason :: string()}.
open(_Filename) ->
    erlang:nif_error("NIF not loaded.").

-spec close(Database :: reference()) -> ok.
close(_Db) ->
    erlang:nif_error("NIF not loaded.").

-spec prepare(Database :: reference(), Query :: string()) ->
    {ok, Stmt :: reference()} | {error, Reason :: string()}.
prepare(_Database, _Query) ->
    erlang:nif_error("NIF not loaded.").

-spec finalize(Stmt :: reference()) -> ok.
finalize(_Stmt) ->
    erlang:nif_error("NIF not loaded.").

-spec reset(Stmt :: reference()) -> ok.
reset(_Stmt) ->
    erlang:nif_error("NIF not loaded.").

-spec step(Stmt :: reference()) -> integer().
step(_Stmt) ->
    erlang:nif_error("NIF not loaded.").

-spec bind(Stmt :: reference(), Param :: integer(), Term :: term()) ->
    ok | {error, Reason :: string()}.
bind(_Stmt, _Param, _Term) ->
    erlang:nif_error("NIF not loaded.").

-spec result(Stmt :: reference(), Param :: integer()) -> ok | {error, Reason :: string()}.
result(_Stmt, _Param) ->
    erlang:nif_error("NIF not loaded.").

-spec bind_count(Stmt :: reference()) -> integer().
bind_count(_Stmt) ->
    erlang:nif_error("NIF not loaded.").

-spec column_count(Stmt :: reference()) -> integer().
column_count(_Stmt) ->
    erlang:nif_error("NIF not loaded.").

-spec column_name(Stmt :: reference(), Column :: integer()) -> atom().
column_name(_Stmt, _Column) ->
    erlang:nif_error("NIF not loaded.").

exec(Db, Query) -> exec(Db, Query, []).
exec(Db, Query, Binding) when is_binary(Query) -> exec(Db, binary_to_list(Query), Binding);
exec(Db, Query, Binding) when is_list(Query) ->
    case erlite:prepare(Db, Query) of
        {error, Reason} ->
            {error, Reason};
        {ok, Stmt} ->
            case exec_bind(Stmt, Binding) of
                ok ->
                    Result = exec_step(Stmt),
                    erlite:finalize(Stmt),
                    Result;
                Error ->
                    Error
            end;
        {ok, Stmt, _Remaining} ->
            erlite:finalize(Stmt),
            {error, "Multiple statements not yet supported."}
    end.

exec_bind(Stmt, Binding) ->
    lists:foldl(
        fun
            ({I, V}, ok) ->
                erlite:bind(Stmt, I, V);
            (_, Error) ->
                Error
        end,
        ok,
        lists:enumerate(Binding)
    ).

exec_step(Stmt) -> exec_step(Stmt, []).
exec_step(Stmt, Acc) ->
    case erlite:step(Stmt) of
        ?SQLITE_DONE ->
            lists:reverse(Acc);
        ?SQLITE_ROW ->
            Result = [
                begin
                    Column = erlite:column_name(Stmt, X - 1),
                    {ok, Res} = erlite:result(Stmt, X - 1),
                    {Column, Res}
                end
             || X <-
                    lists:seq(1, erlite:column_count(Stmt))
            ],
            exec_step(Stmt, [Result | Acc]);
        X ->
            X
    end.

start_test() ->
    {ok, Db} = erlite:open(),
    ok = erlite:close(Db).

prepare_test() ->
    {ok, Db} = erlite:open(),
    {ok, Stmt} = erlite:prepare(Db, "CREATE TABLE test (id int, name text)"),
    ok = erlite:finalize(Stmt),
    ok = erlite:close(Db).

prepare_multiple_test() ->
  {ok, Db} = erlite:open(),
  {ok, Stmt, " INSERT INTO test (id, name) VALUES (1, 'test')"} = 
    erlite:prepare(Db, "CREATE TABLE test (id int, name text); INSERT INTO test (id, name) VALUES (1, 'test')"),
  ok = erlite:finalize(Stmt),
  ok = erlite:close(Db).

step_test() ->
    {ok, Db} = erlite:open(),
    {ok, Stmt} = erlite:prepare(Db, "CREATE TABLE test (id int, name text)"),
    ?SQLITE_DONE = erlite:step(Stmt),
    ok = erlite:reset(Stmt),
    ok = erlite:finalize(Stmt),
    ok = erlite:close(Db).

bind_count_test() ->
    {ok, Db} = erlite:open(),
    {ok, Stmt} = erlite:prepare(Db, "CREATE TABLE test (id int, name text)"),
    ?SQLITE_DONE = erlite:step(Stmt),
    {ok, Stmt1} = erlite:prepare(Db, "INSERT INTO test (id, name) values (?, ?)"),
    2 = erlite:bind_count(Stmt1),
    ok = erlite:finalize(Stmt1),
    ok = erlite:finalize(Stmt),
    ok = erlite:close(Db).

bind_int_test() ->
    {ok, Db} = erlite:open(),
    {ok, Stmt} = erlite:prepare(Db, "CREATE TABLE test (id int, name text)"),
    ?SQLITE_DONE = erlite:step(Stmt),
    {ok, Stmt1} = erlite:prepare(Db, "INSERT INTO test (id, name) values (?, 'test')"),
    1 = erlite:bind_count(Stmt1),
    ok = erlite:bind(Stmt1, 1, 1),
    ok = erlite:finalize(Stmt1),
    ok = erlite:finalize(Stmt),
    ok = erlite:close(Db).

bind_string_test() ->
    {ok, Db} = erlite:open(),
    {ok, Stmt} = erlite:prepare(Db, "CREATE TABLE test (id int, name text)"),
    ?SQLITE_DONE = erlite:step(Stmt),
    {ok, Stmt1} = erlite:prepare(Db, "INSERT INTO test (id, name) values (1, ?)"),
    1 = erlite:bind_count(Stmt1),
    ok = erlite:bind(Stmt1, 1, "test"),
    ok = erlite:finalize(Stmt1),
    ok = erlite:finalize(Stmt),
    ok = erlite:close(Db).

bind_binary_test() ->
    {ok, Db} = erlite:open(),
    {ok, Stmt} = erlite:prepare(Db, "CREATE TABLE test (id int, name blob)"),
    ?SQLITE_DONE = erlite:step(Stmt),
    {ok, Stmt1} = erlite:prepare(Db, "INSERT INTO test (id, name) values (1, ?)"),
    1 = erlite:bind_count(Stmt1),
    ok = erlite:bind(Stmt1, 1, <<"test">>),
    ok = erlite:finalize(Stmt1),
    ok = erlite:finalize(Stmt),
    ok = erlite:close(Db).

result_int_test() ->
    {ok, Db} = erlite:open(),
    {ok, Stmt} = erlite:prepare(Db, "CREATE TABLE test (id int, name blob)"),
    ?SQLITE_DONE = erlite:step(Stmt),
    ok = erlite:finalize(Stmt),
    {ok, Stmt1} = erlite:prepare(Db, "INSERT INTO test (id, name) values (?, NULL)"),
    1 = erlite:bind_count(Stmt1),
    ok = erlite:bind(Stmt1, 1, 255),
    ?SQLITE_DONE = erlite:step(Stmt1),
    ok = erlite:finalize(Stmt1),
    {ok, Stmt2} = erlite:prepare(Db, "SELECT id FROM test"),
    ?SQLITE_ROW = erlite:step(Stmt2),
    1 = erlite:column_count(Stmt2),
    {ok, 255} = erlite:result(Stmt2, 0),
    ?SQLITE_DONE = erlite:step(Stmt2),
    ok = erlite:finalize(Stmt2),
    ok = erlite:close(Db).

result_float_test() ->
    {ok, Db} = erlite:open(),
    {ok, Stmt} = erlite:prepare(Db, "CREATE TABLE test (id real, name blob)"),
    ?SQLITE_DONE = erlite:step(Stmt),
    ok = erlite:finalize(Stmt),
    {ok, Stmt1} = erlite:prepare(Db, "INSERT INTO test (id, name) values (?, NULL)"),
    1 = erlite:bind_count(Stmt1),
    ok = erlite:bind(Stmt1, 1, 2.2),
    ?SQLITE_DONE = erlite:step(Stmt1),
    ok = erlite:finalize(Stmt1),
    {ok, Stmt2} = erlite:prepare(Db, "SELECT id FROM test"),
    ?SQLITE_ROW = erlite:step(Stmt2),
    1 = erlite:column_count(Stmt2),
    {ok, 2.2} = erlite:result(Stmt2, 0),
    ok = erlite:finalize(Stmt2),
    ok = erlite:close(Db).

result_list_test() ->
    {ok, Db} = erlite:open(),
    {ok, Stmt} = erlite:prepare(Db, "CREATE TABLE test (id int, name test)"),
    ?SQLITE_DONE = erlite:step(Stmt),
    ok = erlite:finalize(Stmt),
    {ok, Stmt1} = erlite:prepare(Db, "INSERT INTO test (id, name) values (1, ?)"),
    1 = erlite:bind_count(Stmt1),
    ok = erlite:bind(Stmt1, 1, "test"),
    ?SQLITE_DONE = erlite:step(Stmt1),
    ok = erlite:finalize(Stmt1),
    {ok, Stmt2} = erlite:prepare(Db, "SELECT name FROM test"),
    ?SQLITE_ROW = erlite:step(Stmt2),
    1 = erlite:column_count(Stmt2),
    {ok, "test"} = erlite:result(Stmt2, 0),
    ok = erlite:finalize(Stmt2),
    ok = erlite:close(Db).

result_binary_test() ->
    {ok, Db} = erlite:open(),
    {ok, Stmt} = erlite:prepare(Db, "CREATE TABLE test (id int, name blob)"),
    ?SQLITE_DONE = erlite:step(Stmt),
    ok = erlite:finalize(Stmt),
    {ok, Stmt1} = erlite:prepare(Db, "INSERT INTO test (id, name) values (1, ?)"),
    1 = erlite:bind_count(Stmt1),
    ok = erlite:bind(Stmt1, 1, <<"test">>),
    ?SQLITE_DONE = erlite:step(Stmt1),
    ok = erlite:finalize(Stmt1),
    {ok, Stmt2} = erlite:prepare(Db, "SELECT name FROM test"),
    ?SQLITE_ROW = erlite:step(Stmt2),
    1 = erlite:column_count(Stmt2),
    {ok, <<"test">>} = erlite:result(Stmt2, 0),
    ok = erlite:finalize(Stmt2),
    ok = erlite:close(Db).

multi_statement_test() ->
  {ok, Db} = erlite:open(),
  {error, _} = erlite:exec(Db, "CREATE TABLE test (id int, name blob); INSERT INTO test (id, blob) VALUES (1, ?)", [<<"name">>]).

exec_test() ->
    {ok, Db} = erlite:open(),
    [] = erlite:exec(Db, "CREATE TABLE test (id int, name text)"),
    [] = erlite:exec(Db, "INSERT INTO test VALUES (0, 'testname')"),
    [] = erlite:exec(Db, "INSERT INTO test VALUES (1, NULL)"),
    [[{id, 0}, {name, "testname"}], [{id, 1}, {name, null}]] = erlite:exec(
        Db, "SELECT id, name FROM test"
    ),
    ok = erlite:close(Db).

exec_bind_test() ->
    {ok, Db} = erlite:open(),
    [] = erlite:exec(Db, "CREATE TABLE test (id int, name text)"),
    [] = erlite:exec(Db, "INSERT INTO test VALUES (0, ?)", ["testname"]),
    [] = erlite:exec(Db, "INSERT INTO test VALUES (1, NULL)"),
    [[{id, 0}, {name, "testname"}], [{id, 1}, {name, null}]] = erlite:exec(
        Db, "SELECT id, name FROM test"
    ),
    ok = erlite:close(Db).
