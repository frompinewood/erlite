erlite
=====

An Erlang NIF for sqlite3. 

Build
-----

    $ rebar3 compile

Usage
-----
```erlang
{ok, Db} = erlite:open(":memory:"),
[] = erlite:exec(Db, "CREATE TABLE users (id INT, name TEXT)"),
[] = erlite:exec(Db, "INSERT INTO users (id, name) VALUES (0, 'test')"),
[] = erlite:exec(Db, "INSERT INTO users (id, name) VALUES (1, ?)", ["other"]),
[[{id, 0}, {name, "test"}], [{id, 1}, {name, "other"}]] = 
    erlite:exec(Db, "SELECT * FROM users").
ok = erlite:close(Db).
```
