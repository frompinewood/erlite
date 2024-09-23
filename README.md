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
ok = erlite:close(Db).
```
