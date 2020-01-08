#include <string.h>
#include <iostream>
#include <sqlite3.h>

#include "common.h"

void create_new_table(sqlite3 *db, TableType table_type, const char *table_name) {
    const char *table_definition;

    if (table_type == Trade) {
        table_definition =
            "'timestamp' INTEGER NOT NULL,"
            "'price' REAL NOT NULL,"
            "'size' REAL NOT NULL";
            
    } else if (table_type == Book) {
        table_definition =
            "'timestamp' INTEGER NOT NULL,"
            "'price' REAL NOT NULL,"
            "'size' REAL NOT NULL";

    } else if (table_type == Ticker) {
        table_definition =
            "'timestamp' INTEGER NOT NULL,"
            "'best_bid' REAL NOT NULL,"
            "'best_bid_size' REAL NOT NULL,"
            "'total_bid_depth' REAL NOT NULL,"
            "'best_ask' REAL NOT NULL,"
            "'best_ask_size' REAL NOT NULL,"
            "'total_ask_depth' REAL NOT NULL,"
            "'last_traded_price' REAL NOT NULL,"
            "'volume' REAL NOT NULL,"
            "'volume_by_product' REAL NOT NULL";

    } else {
        std::cerr << "table type?" << std::endl;
        exit(1);
    }

    int r;
    char *err;
    char *sql = (char *) malloc(sizeof(char)*N_SQL);
    
    snprintf(sql, N_SQL, "CREATE TABLE IF NOT EXISTS '%s' (%s)", table_name, table_definition);

    r = sqlite3_exec(db, sql, NULL, NULL, &err);

    if (r != SQLITE_OK) {
        std::cout << "sqlite error: " << err << std::endl;
        sqlite3_free(err);
        exit(1);
    }

    free(sql);
}
