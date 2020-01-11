#include <sqlite3.h>
#include <rapidjson/document.h>
#include <map>

#include "common.h"
#include "bitmex.h"

void bitmex_trade(sqlite3 *db, unsigned long long line_timestamp, rapidjson::Document &doc) {
    const char *action = doc["action"].GetString();

    auto data = doc["data"].GetArray();


    if (strcmp(action, "partial") == 0) {
        char *table_name = (char *) malloc(sizeof(char)*N_PAIR);
        
        // data is full with currency pairs in bitmex
        // size of all of them is 0, it is fake trade
        // just to notify what pairs they have
        for (auto i = data.begin(); i != data.end(); i++) {
            const char *symbol = (*i)["symbol"].GetString();

            // create new table
            snprintf(table_name, N_PAIR, "trade_%s", symbol);

            create_new_table(db, Trade, table_name);
        }

        free(table_name);
    } else if (strcmp(action, "insert") == 0) {
        char *sql = (char *) malloc(sizeof(char)*N_SQL);
        
        for (auto i = data.begin(); i != data.end(); i++) {
            const char *symbol = (*i)["symbol"].GetString();
            const char *side = (*i)["side"].GetString();
            int64_t size = (*i)["size"].GetUint64();
            double price = (*i)["price"].GetDouble();

            // negate size if sell
            if (strcmp(side, "Sell") == 0) {
                size = -size;
            }

            snprintf(sql, N_SQL, "INSERT INTO 'trade_%s' VALUES (%llu, %.10f, %lu)", symbol, line_timestamp, price, size);

            execute_insert(db, sql);
        }

        free(sql);
    } else {
        std::cerr << "unknown action: " << action << std::endl;
        exit(1);
    }
}

struct Order {
    const char *symbol;
    unsigned long id;

    bool operator==(const Order &o) {
        return strcmp(symbol, o.symbol) == 0 && id == o.id;
    }
    bool operator<(const Order &o) {
        int cmp = strcmp(symbol, o.symbol);
        return cmp || (cmp == 0 && id < o.id); 
    }
};

std::map<struct Order*, double> ob_id_order;

void bitmex_orderbook(sqlite3 *db, unsigned long long line_timestamp, rapidjson::Document &doc) {
    const char *action = doc["action"].GetString();

    auto data = doc["data"].GetArray();

    char *sql = (char *) malloc(sizeof(char)*N_SQL);

    if (strcmp(action, "partial") == 0) {
        // first full orderbook snapshot
        char *table_name = (char *) malloc(sizeof(char)*N_PAIR);

        for (auto i = data.begin(); i != data.end(); i++) {
            const char *symbol = (*i)["symbol"].GetString();
            double price = (*i)["price"].GetDouble();
            int64_t size = (*i)["size"].GetUint64();

            unsigned long id = (*i)["id"].GetUint64();
            const char *side = (*i)["side"].GetString();

            // copy symbol for storing into a map
            char *symbol_cpy = (char *) malloc(sizeof(char)*N_PAIR);
            strcpy(symbol_cpy, symbol);

            struct Order *order = (struct Order*) malloc(sizeof(struct Order));
            order->symbol = symbol_cpy;
            order->id = id;

            // set price to a map for tracking
            ob_id_order[order] = price;

            /* insert into a database */
            // if sell is 1 (true) then -size, 0 then size
            if (strcmp(side, "Sell") == 0) {
                size = -size;
            }

            // create new table
            snprintf(table_name, N_PAIR, "orderBookL2_%s", symbol);
            create_new_table(db, Book, table_name);

            // insert
            snprintf(sql, N_SQL, "INSERT INTO '%s' VALUES (%llu, %.10f, %lu)", table_name, line_timestamp, price, size);
            execute_insert(db, sql);
        }

        free(table_name);
    } else if (strcmp(action, "update") == 0 || strcmp(action, "insert") == 0) {
        for (auto i = data.begin(); i != data.end(); i++) {
            const char *symbol = (*i)["symbol"].GetString();
            int64_t size = (*i)["size"].GetUint64();

            unsigned long id = (*i)["id"].GetUint64();
            const char *side = (*i)["side"].GetString();

            // copy symbol for getting price
            char *symbol_cpy = (char *) malloc(sizeof(char)*N_PAIR);
            strcpy(symbol_cpy, symbol);

            // get price for symbol and id
            struct Order order = Order{symbol_cpy, id};
            double price = ob_id_order[&order];

            free(symbol_cpy);

            /* insert into a database */
            if (strcmp(side, "Sell") == 0) {
                size = -size;
            }

            // insert
            snprintf(sql, N_SQL, "INSERT INTO 'orderBookL2_%s' VALUES (%llu, %.10f, %lu)", symbol, line_timestamp, price, size);
            execute_insert(db, sql);
        }
    } else if (strcmp(action, "delete") == 0) {
        for (auto i = data.begin(); i != data.end(); i++) {
            const char *symbol = (*i)["symbol"].GetString();

            unsigned long id = (*i)["id"].GetUint64();

            // copy symbol for getting price
            char *symbol_cpy = (char *) malloc(sizeof(char)*N_PAIR);
            strcpy(symbol_cpy, symbol);

            // get price for symbol and id
            struct Order order = Order{symbol_cpy, id};
            double price = ob_id_order[&order];
            // deleted, erase from map
            ob_id_order.erase(&order);

            free(symbol_cpy);

            /* insert into a database */
            // insert
            snprintf(sql, N_SQL, "INSERT INTO 'orderBookL2_%s' VALUES (%llu, %.10f, 0)", symbol, line_timestamp, price);
            execute_insert(db, sql);
        }
    } else {
        std::cerr << "unknown action: " << action << std::endl;
        free(sql);
        exit(1);
    }

    free(sql);
}

void bitmex_emit(sqlite3 *db, unsigned long long line_timestamp, rapidjson::Document &doc) {
}

void bitmex_msg(sqlite3 *db, unsigned long long line_timestamp, rapidjson::Document &doc) {
    if (!doc.IsObject()) {
        std::cerr << "not object" << std::endl;
        exit(1);
    }
    if (doc.HasMember("info") && doc["info"].IsString()) {
        // welcome message, ignore
        return;
    }
    if (doc.HasMember("success")) {
        // a response to subscription
        return;
    }
    if (doc.HasMember("error")) {
        const char *error = doc["error"].GetString();
        
        if (strncmp(error, "You are already subscribed to this topic:", strlen("You are already subscribed to this topic:")) == 0) {
            // just a duplicate subscription, ignore
            return;
        } else {
            std::cerr << "client error: " << error << std::endl;
            exit(1);
        }
    }

    const char *table = doc["table"].GetString();

    if (strcmp(table, "orderBookL2") == 0) {
        bitmex_orderbook(db, line_timestamp, doc);
    } else if (strcmp(table, "trade") == 0) {
        bitmex_trade(db, line_timestamp, doc);
    } else if (strcmp(table, "announcement") == 0 ||
        strcmp(table, "chat") == 0 ||
        strcmp(table, "connected") == 0 ||
        strcmp(table, "publicNotifications") == 0 ||
        strcmp(table, "instrument") == 0 ||
        strcmp(table, "insurance") == 0 ||
        strcmp(table, "funding") == 0 ||
        strcmp(table, "liquidation") == 0 ||
        strcmp(table, "settlement") == 0) {
        // ignore
        return;
    } else {
        std::cerr << "unknown table: " << table << std::endl;
        exit(1);
    }
}
