#ifndef ERLITE_H
#define ERLITE_H 

#include <erl_nif.h>
#include <sqlite3.h>

sqlite3* erl_term_to_db(ErlNifEnv* env, ERL_NIF_TERM term);



#endif
