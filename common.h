#ifndef COMMON_H
#define COMMON_H

#include <iostream>
#include <sqlite3.h>
#include <rapidjson/document.h>

#define N_L 10000000
#define N_PAIR 128
#define N_SQL 512
#define N_ERR 512
#define BID 0
#define ASK 1
#define BUY 0
#define ASK 1

enum TableType{
    Trade,
    Book,
    Ticker,
};

void create_new_table(sqlite3 *db, TableType table_type, const char *table_name);

inline void execute_insert(sqlite3 *db, const char *sql) {
    int r;
    char *err;

    r = sqlite3_exec(db, sql, NULL, NULL, &err);

    if (r != SQLITE_OK) {
        std::cerr << "sqlite error: " << err << std::endl;
        sqlite3_free(err);
        exit(1);
    }
}

inline void start_transaction(sqlite3 *db) {
    int r;
    char *err;

    // setup new transaction
    r = sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, &err);

    if (r != SQLITE_OK) {
        std::cerr << "starting new transaction failed: " << err << std::endl;
        sqlite3_free(err);
        exit(1);
    }
}

inline void commit(sqlite3 *db) {
    int r;
    char *err;
    
    r = sqlite3_exec(db, "COMMIT", NULL, NULL, &err);

    if (r != SQLITE_OK) {
        std::cerr << "commit failed: " << err << std::endl;
        sqlite3_free(err);
        exit(1);
    }
}

#endif
