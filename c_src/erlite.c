#include <erl_nif.h>
#include <sqlite3.h>

ErlNifResourceType* DB_HANDLE_TYPE;

static void db_handle_dtor(ErlNifEnv* env, void* res) {
  sqlite3_close(*(sqlite3**)res);
}

static int on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_data) {
  DB_HANDLE_TYPE = enif_open_resource_type(env, "erlite", "erlite_db", db_handle_dtor, 
                                           ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);
  return 0;
}

static ERL_NIF_TERM erlite_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  unsigned int length;
  if (!enif_get_string_length(env, argv[0], &length, ERL_NIF_UTF8)) return enif_make_badarg(env);
  length++;
  char filename[length];
  if (!enif_get_string(env, argv[0], filename, length, ERL_NIF_UTF8)) return enif_make_badarg(env);
  sqlite3* db; 
  if (sqlite3_open(filename, &db) != SQLITE_OK) 
    return enif_make_tuple2(env, enif_make_atom(env, "error"), enif_make_string(env, sqlite3_errmsg(db), ERL_NIF_UTF8));
  sqlite3** db_res = enif_alloc_resource(DB_HANDLE_TYPE, sizeof(sqlite3*));
  *db_res = db;
  return enif_make_tuple2(env, enif_make_atom(env, "ok"), enif_make_resource(env, db_res));
}

static ERL_NIF_TERM erlite_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  sqlite3** db_res;
  if(!enif_get_resource(env, argv[0], DB_HANDLE_TYPE, (void*)&db_res)) return enif_make_badarg(env);
  enif_release_resource(db_res);
  return enif_make_atom(env, "ok");
}

static ErlNifFunc nif_funcs[] = {
  {"open", 1, erlite_open},
  {"close", 1, erlite_close}
};

ERL_NIF_INIT(erlite, nif_funcs, on_load, NULL, NULL, NULL)
