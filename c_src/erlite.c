#include <erl_nif.h>
#include <sqlite3.h>
#include <string.h>

ErlNifResourceType* DB_HANDLE_TYPE;
ErlNifResourceType* STMT_HANDLE_TYPE;

union ErliteType {
  int i;
  char* c;
  double d;
};

typedef union ErliteType ET;

static void db_handle_dtor(ErlNifEnv* env, void* res) {
  sqlite3_close(*(sqlite3**)res);
}

static void stmt_handle_dtor(ErlNifEnv* env, void* res) {
  sqlite3_finalize(*(sqlite3_stmt**)res);
}

static int on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_data) {
  DB_HANDLE_TYPE = enif_open_resource_type(env, "erlite", "erlite_db", db_handle_dtor, 
                                           ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);
  STMT_HANDLE_TYPE = enif_open_resource_type(env, "erlite", "erlite_stmt", stmt_handle_dtor,
                                          ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);
  return 0;
}

static ERL_NIF_TERM erlite_stmt_error(ErlNifEnv* env, sqlite3_stmt* stmt) {
  return enif_make_tuple2(env, 
          enif_make_atom(env, "error"), 
          enif_make_string(env, sqlite3_errmsg(sqlite3_db_handle(stmt)), ERL_NIF_UTF8));

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

static ERL_NIF_TERM erlite_prepare(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  sqlite3** db_res;
  if(!enif_get_resource(env, argv[0], DB_HANDLE_TYPE, (void*)&db_res)) return enif_make_badarg(env);
  sqlite3* db = *db_res;

  unsigned int length;
  if (!enif_get_string_length(env, argv[1], &length, ERL_NIF_UTF8)) return enif_make_badarg(env);
  length++;
  char query[length];
  if (!enif_get_string(env, argv[1], query, length, ERL_NIF_UTF8)) return enif_make_badarg(env);

  sqlite3_stmt* stmt;
  sqlite3_stmt** stmt_res;

  if (sqlite3_prepare_v2(db, query, length, &stmt, NULL) != SQLITE_OK) 
    return enif_make_tuple2(env, enif_make_atom(env, "error"), enif_make_string(env, sqlite3_errmsg(db), ERL_NIF_UTF8));

  stmt_res = enif_alloc_resource(STMT_HANDLE_TYPE, sizeof(sqlite3_stmt*));
  *stmt_res = stmt;

  return enif_make_tuple2(env, enif_make_atom(env, "ok"), enif_make_resource(env, stmt_res));
}

static ERL_NIF_TERM erlite_reset(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  sqlite3_stmt** stmt_res;
  sqlite3_stmt* stmt;
  if (!enif_get_resource(env, argv[0], STMT_HANDLE_TYPE, (void*)&stmt_res)) return enif_make_badarg(env);
  stmt = *stmt_res;
  sqlite3_reset(stmt);
  return enif_make_atom(env, "ok");
}

static ERL_NIF_TERM erlite_step(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  sqlite3_stmt** stmt_res;
  sqlite3_stmt* stmt;
  if (!enif_get_resource(env, argv[0], STMT_HANDLE_TYPE, (void*)&stmt_res)) return enif_make_badarg(env);
  stmt = *stmt_res;
  return enif_make_int(env, sqlite3_step(stmt));
}

static ERL_NIF_TERM erlite_finalize(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  sqlite3_stmt** stmt_res;
  if (!enif_get_resource(env, argv[0], STMT_HANDLE_TYPE, (void*)&stmt_res)) return enif_make_badarg(env);
  enif_release_resource(stmt_res);
  return enif_make_atom(env, "ok");
}

static ERL_NIF_TERM erlite_bind_int(sqlite3_stmt* stmt, ErlNifEnv* env, int param, ERL_NIF_TERM term) {
  int value;
  if (!enif_get_int(env, term, &value)) return enif_make_badarg(env);
  if (sqlite3_bind_int(stmt, param, value) != SQLITE_OK)
    return erlite_stmt_error(env, stmt);
  return enif_make_atom(env, "ok");
}

static ERL_NIF_TERM erlite_bind_float(sqlite3_stmt* stmt, ErlNifEnv* env, int param, ERL_NIF_TERM term) {
  double value;
  if (!enif_get_double(env, term, &value)) return enif_make_badarg(env);
  if (sqlite3_bind_double(stmt, param, value) != SQLITE_OK)
    return erlite_stmt_error(env, stmt);
  return enif_make_atom(env, "ok");
}

static ERL_NIF_TERM erlite_bind_string(sqlite3_stmt* stmt, ErlNifEnv* env, int param, ERL_NIF_TERM term) {
  unsigned int len;
  if (!enif_get_string_length(env, term, &len, ERL_NIF_UTF8)) return enif_make_badarg(env);
  char buf[++len];
  if (!enif_get_string(env, term, buf, len, ERL_NIF_UTF8)) return enif_make_badarg(env);
  if (sqlite3_bind_text(stmt, param, buf, len, SQLITE_TRANSIENT) != SQLITE_OK)
    return erlite_stmt_error(env, stmt);
  return enif_make_atom(env, "ok");
}

static ERL_NIF_TERM erlite_bind_atom(sqlite3_stmt* stmt, ErlNifEnv* env, int param, ERL_NIF_TERM term) {
  unsigned int len;
  if (!enif_get_atom_length(env, term, &len, ERL_NIF_UTF8)) return enif_make_badarg(env);
  char buf[++len];
  if (!enif_get_atom(env, term, buf, len, ERL_NIF_UTF8)) return enif_make_badarg(env);
  if (sqlite3_bind_text(stmt, param, buf, len, SQLITE_TRANSIENT) != SQLITE_OK)
    return erlite_stmt_error(env, stmt);
  return enif_make_atom(env, "ok");
}

static ERL_NIF_TERM erlite_bind_binary(sqlite3_stmt* stmt, ErlNifEnv* env, int param, ERL_NIF_TERM term) {
  ErlNifBinary bin;
  if (!enif_term_to_binary(env, term, &bin)) 
    return enif_make_tuple2(env, 
            enif_make_atom(env, "ok"), 
            enif_make_string(env, "binary allocation failed", ERL_NIF_UTF8));
  if (sqlite3_bind_blob(stmt, param, (void*)bin.data, bin.size, SQLITE_TRANSIENT) != SQLITE_OK) 
    return erlite_stmt_error(env, stmt);
  return enif_make_atom(env, "ok");
}

static ERL_NIF_TERM erlite_bind(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  sqlite3_stmt* stmt;
  sqlite3_stmt** stmt_res;
  int param;
  if (!enif_get_resource(env, argv[0], STMT_HANDLE_TYPE, (void*)&stmt_res)) return enif_make_badarg(env);
  stmt = *stmt_res;
  if (!enif_get_int(env, argv[1], &param)) return enif_make_badarg(env);
  switch (enif_term_type(env, argv[2])) {
    case ERL_NIF_TERM_TYPE_INTEGER:
      return erlite_bind_int(stmt, env, param, argv[2]);
      break;
    case ERL_NIF_TERM_TYPE_FLOAT:
      return erlite_bind_float(stmt, env, param, argv[2]);
      break;
    case ERL_NIF_TERM_TYPE_LIST:
      return erlite_bind_string(stmt, env, param, argv[2]);
      break;
    case ERL_NIF_TERM_TYPE_ATOM:
      return erlite_bind_atom(stmt, env, param, argv[2]);
      break;
    case ERL_NIF_TERM_TYPE_BITSTRING:
      if (!enif_is_binary(env, argv[2])) return enif_make_badarg(env);
      return erlite_bind_binary(stmt, env, param, argv[2]);
      break;
    default:
      return enif_make_badarg(env);
      break;
  }
}
 
static ERL_NIF_TERM erlite_result_int(ErlNifEnv* env, sqlite3_stmt* stmt, int column) {
  int value = sqlite3_column_int(stmt, column);
  return enif_make_tuple2(env, enif_make_atom(env, "ok"), enif_make_int(env, value));
}

static ERL_NIF_TERM erlite_result_float(ErlNifEnv* env, sqlite3_stmt* stmt, int column) {
  double value = sqlite3_column_double(stmt, column);
  return enif_make_tuple2(env, enif_make_atom(env, "ok"), enif_make_double(env, value));
}

static ERL_NIF_TERM erlite_result_text(ErlNifEnv* env, sqlite3_stmt* stmt, int column) {
  if (sqlite3_column_bytes(stmt, column) == 0) return enif_make_tuple2(env, enif_make_atom(env, "ok"), enif_make_atom(env, "null"));
  char* value = (char*) sqlite3_column_text(stmt, column);
  return enif_make_tuple2(env, enif_make_atom(env, "ok"), enif_make_string(env, value, ERL_NIF_UTF8));  
}

static ERL_NIF_TERM erlite_result_blob(ErlNifEnv* env, sqlite3_stmt* stmt, int column) {
  ErlNifBinary bin;
  ERL_NIF_TERM term;
  bin.size = sqlite3_column_bytes(stmt, column);
  if (bin.size == 0) return enif_make_tuple2(env, enif_make_atom(env, "ok"), enif_make_atom(env, "null"));
  enif_alloc_binary(bin.size, &bin);
  bin.data = (unsigned char*)sqlite3_column_blob(stmt, column);
  enif_binary_to_term(env, bin.data, bin.size, &term, 0);
  enif_release_binary(&bin);
  return enif_make_tuple2(env, enif_make_atom(env, "ok"), term);  
}

static ERL_NIF_TERM erlite_result(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  sqlite3_stmt* stmt;
  sqlite3_stmt** stmt_res;
  int column;
  if (!enif_get_resource(env, argv[0], STMT_HANDLE_TYPE, (void*)&stmt_res)) return enif_make_badarg(env);
  stmt = *stmt_res;
  if (!enif_get_int(env, argv[1], &column)) return enif_make_badarg(env);

  switch (sqlite3_column_type(stmt, column)) {
    case SQLITE_INTEGER:
      return erlite_result_int(env, stmt, column); 
      break;
    case SQLITE_FLOAT:
      return erlite_result_float(env, stmt, column);
      break;
    case SQLITE_TEXT:
      return erlite_result_text(env, stmt, column);
      break;
    case SQLITE_BLOB:
      return erlite_result_blob(env, stmt, column);
      break;
    case SQLITE_NULL:
      return enif_make_tuple2(env, enif_make_atom(env, "ok"), enif_make_atom(env, "null"));
    default:
      if (sqlite3_errcode(sqlite3_db_handle(stmt)))
        return enif_make_tuple2(env, enif_make_atom(env, "error"), enif_make_string(env, sqlite3_errmsg(sqlite3_db_handle(stmt)), ERL_NIF_UTF8));
      return enif_make_tuple2(env, enif_make_atom(env, "error"), enif_make_string(env, "Column type not supported", ERL_NIF_UTF8));
      break;
  }
  return enif_make_badarg(env);
}

static ERL_NIF_TERM erlite_bind_count(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  sqlite3_stmt* stmt;
  sqlite3_stmt** stmt_res;
  if (!enif_get_resource(env, argv[0], STMT_HANDLE_TYPE, (void*)&stmt_res)) return enif_make_badarg(env);
  stmt = *stmt_res;
  return enif_make_int(env, sqlite3_bind_parameter_count(stmt));
}

static ERL_NIF_TERM erlite_column_count(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  sqlite3_stmt* stmt;
  sqlite3_stmt** stmt_res;
  if (!enif_get_resource(env, argv[0], STMT_HANDLE_TYPE, (void*)&stmt_res)) return enif_make_badarg(env);
  stmt = *stmt_res;
  return enif_make_int(env, sqlite3_data_count(stmt));
}

static ERL_NIF_TERM erlite_column_name(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  sqlite3_stmt* stmt;
  sqlite3_stmt** stmt_res;
  int column;
  if (!enif_get_resource(env, argv[0], STMT_HANDLE_TYPE, (void*)&stmt_res)) return enif_make_badarg(env);
  stmt = *stmt_res;
  if (!enif_get_int(env, argv[1], &column)) return enif_make_badarg(env);
  return enif_make_atom(env, sqlite3_column_name(stmt, column));
}


static ErlNifFunc nif_funcs[] = {
  {"open", 1, erlite_open},
  {"close", 1, erlite_close},
  {"prepare", 2, erlite_prepare},
  {"finalize", 1, erlite_finalize},
  {"reset", 1, erlite_reset},
  {"step", 1, erlite_step},
  {"bind", 3, erlite_bind},
  {"result", 2, erlite_result},
  {"bind_count", 1, erlite_bind_count},
  {"column_count", 1, erlite_column_count},
  {"column_name", 2, erlite_column_name}
};

ERL_NIF_INIT(erlite, nif_funcs, on_load, NULL, NULL, NULL)
