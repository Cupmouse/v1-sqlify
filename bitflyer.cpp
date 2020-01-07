#include <ctime>
#include <sqlite3.h>
#include <rapidjson/document.h>
#include <iostream>
#include <set>

#include "bitflyer.h"
#include "./common.h"

using namespace rapidjson;

inline unsigned long long bitflyer_time(const char *str) {
    struct tm time;
    unsigned long long nanosec;

    memset(&time, 0, sizeof(struct tm));

    time.tm_isdst = -1;
    strptime(str, "%Y-%m-%dT%H:%M:%S", &time);

    nanosec = ((unsigned long long) timegm(&time)) * 1000000000;
    nanosec += atol(str+strlen("2020-01-01T19:12:03.")) * 1000;

    return nanosec;
}

inline void bitflyer_executions(sqlite3 *db,
    unsigned long long line_timestamp,
    const char *channel,
    rapidjson::GenericArray<false, rapidjson::Value::ValueType> &array) {

    // process messages
    int r;
    char *err = NULL;
    char *sql = (char *) malloc(sizeof(char)*N_SQL);

    const char *sideUpper;
    unsigned long long time;
    // negative if sell, positive if buy
    double size;
    double price;
    
    for (auto i = array.begin(); i != array.end(); i++) {
        auto obj = i->GetObject();

        if (obj["side"].GetStringLength() > 0) {
            sideUpper = obj["side"].GetString();

            // flip sign to be negative if the side is sell
            // stays the same if buy
            size = size*(1-(sideUpper[0] == 'S')*2);

            // optional check for side illegality
            if (sideUpper[0] != 'B' && sideUpper[0] != 'S') {
                // unknown side!
                std::cerr << "execution: unknown side " << sideUpper << std::endl;
                exit(1);
            }
        } else {
            // empty side string means itayose
            // TODO ignoring itayose for now
            continue;
        }
        
        time = bitflyer_time(obj["exec_date"].GetString());
        price = obj["price"].GetDouble();
        size = obj["size"].GetDouble();

        // construct sql
        snprintf(sql, N_SQL, "INSERT INTO '%s' VALUES(%llu, %.10f, %.10f)", channel, time, price, size);

        r = sqlite3_exec(db, sql, NULL, NULL, &err);

        if (r != SQLITE_OK) {
            std::cerr << "sqlite error: " << err << std::endl;
            sqlite3_free(err);
            exit(1);
        }
    }

    free(sql);
}

// side is 0 if buy, 1 if sell
inline void bitflyer_board_side(sqlite3 *db,
    unsigned long long line_timestamp,
    const char *table_name,
    rapidjson::GenericArray<false, rapidjson::Value::ValueType> &array,
    const int side) {

    int r;
    char *err;
    char *sql = (char*) malloc(sizeof(char)*N_SQL);

    for (auto i = array.begin(); i != array.end(); i++) {
        auto obj = i->GetObject();

        double price = obj["price"].GetDouble();
        double size = obj["size"].GetDouble();

        // flip sign to be negative if the side is sell
        // stays the same if buy
        size = size*(1-side*2);

        snprintf(sql, N_SQL, "INSERT INTO '%s' VALUES(%llu, %.10f, %.10f)", table_name, line_timestamp, price, size);

        r = sqlite3_exec(db, sql, NULL, NULL, &err);

        if (r != SQLITE_OK) {
            std::cerr << "sqlite error: " << err << std::endl;
            sqlite3_free(err);
            exit(1);
        }
    }

    free(sql);
}

inline void bitflyer_board_snapshot(sqlite3 *db,
    unsigned long long line_timestamp,
    const char *channel,
    rapidjson::GenericObject<false, rapidjson::Value> &obj) {

    char *table_name = (char *) malloc(sizeof(char)*N_PAIR);

    // insert into board table, not board_snapshot table
    // set prefix
    strcpy(table_name, "lightning_board_");
    // skip prefix to get pair name and append it to table_name
    strcat(table_name, channel + strlen("lightning_board_snapshot_"));

    auto bids_array = obj["bids"].GetArray();
    auto asks_array = obj["asks"].GetArray();

    bitflyer_board_side(db, line_timestamp, table_name, bids_array, 0);
    bitflyer_board_side(db, line_timestamp, table_name, asks_array, 1);

    free(table_name);
}

inline void bitflyer_board(sqlite3 *db,
    unsigned long long line_timestamp,
    const char *channel,
    rapidjson::GenericObject<false, rapidjson::Value> &obj) {

    auto bids_array = obj["bids"].GetArray();
    auto asks_array = obj["asks"].GetArray();

    bitflyer_board_side(db, line_timestamp, channel, bids_array, 0);
    bitflyer_board_side(db, line_timestamp, channel, asks_array, 1);
}

inline void bitflyer_ticker(sqlite3 *db,
    unsigned long long line_timestamp,
    const char *channel,
    rapidjson::GenericObject<false, rapidjson::Value> &obj) {

    unsigned long long timestamp = bitflyer_time(obj["timestamp"].GetString());
    double best_bid = obj["best_bid"].GetDouble();
    double best_bid_size = obj["best_bid_size"].GetDouble();
    double total_bid_depth = obj["total_bid_depth"].GetDouble();
    double best_ask = obj["best_ask"].GetDouble();
    double best_ask_size = obj["best_ask_size"].GetDouble();
    double total_ask_depth = obj["total_ask_depth"].GetDouble();
    double last_traded_price = obj["ltp"].GetDouble();
    double volume = obj["volume"].GetDouble();
    double volume_by_product = obj["volume_by_product"].GetDouble();

    int r;
    char *err = NULL;
    char *sql = (char *) malloc(sizeof(char)*N_SQL);

    snprintf(sql, N_SQL,
        "INSERT INTO '%s' VALUES(%llu, %.10f, %.7f, %.12f, %.10f, %.7f, %.12f, %.10f, %.12f, %.12f)",
        channel,
        timestamp,
        best_bid,
        best_bid_size,
        total_bid_depth,
        best_ask,
        best_ask_size,
        total_ask_depth,
        last_traded_price,
        volume,
        volume_by_product);

    r = sqlite3_exec(db, sql, NULL, NULL, &err);

    if (r != SQLITE_OK) {
        std::cerr << "sqlite error: " << err << std::endl;
        sqlite3_free(err);
        exit(1);
    }

    free(sql);
}

void bitflyer_emit(sqlite3 *db, unsigned long long line_timestamp, Document &doc) {
    const char *channel = doc["params"]["channel"].GetString();
    const char *table_definition;

    if (strncmp(channel, "lightning_executions_", strlen("lightning_executions_")) == 0) {
        table_definition =
            "'timestamp' INTEGER NOT NULL,"
            "'price' REAL NOT NULL,"
            "'size' REAL NOT NULL";
            
    } else if (strncmp(channel, "lightning_board_snapshot_", strlen("lightning_board_snapshot_")) == 0) {
        return; // do nothing

    } else if (strncmp(channel, "lightning_board_", strlen("lightning_board_")) == 0) {
        table_definition =
            "'timestamp' INTEGER NOT NULL,"
            "'price' REAL NOT NULL,"
            "'size' REAL NOT NULL";

    } else if (strncmp(channel, "lightning_ticker_", strlen("lightning_ticker_")) == 0) {
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
        std::cerr << "unknown channel prefix: " << channel << std::endl;
        exit(1);
    }

    int r;
    char *err;
    char *sql = (char *) malloc(sizeof(char)*N_SQL);
    
    snprintf(sql, N_SQL, "CREATE TABLE IF NOT EXISTS '%s' (%s)", channel, table_definition);

    r = sqlite3_exec(db, sql, NULL, NULL, &err);

    if (r != SQLITE_OK) {
        std::cout << "sqlite error: " << err << std::endl;
        sqlite3_free(err);
        exit(1);
    }

    free(sql);
}

void bitflyer_msg(sqlite3 *db, unsigned long long line_timestamp, Document &doc) {
    if (!doc.IsObject()) {
        // not an valid json
        std::cerr << "not a object" << std::endl;
        return;
    }
    if (doc.HasMember("result")) {
        // if result key exists, then this message is a reply to subscription
        if (!doc["result"].GetBool()) {
            std::cerr << "subscription result is false" << std::endl;
            exit(1);
        }

        return;
    }
    if (!doc.HasMember("method")) {
        std::cerr << "no method" << std::endl;
        return;
    }

    const char *method = doc["method"].GetString();

    if (strcmp(method, "channelMessage") != 0) {
        // not an important message
        std::cerr << "not a channelMessage" << std::endl;
        return;
    }

    auto &params = doc["params"];
    const char *channel = params["channel"].GetString();

    if (strncmp(channel, "lightning_executions_", strlen("lightning_executions_")) == 0) {
        // executions
        auto array = params["message"].GetArray();

        bitflyer_executions(db, line_timestamp, channel, array);
    } else if (strncmp(channel, "lightning_board_snapshot_", strlen("lightning_board_snapshot_")) == 0) {
        auto obj = params["message"].GetObject();

        bitflyer_board_snapshot(db, line_timestamp, channel, obj);
    } else if (strncmp(channel, "lightning_board_", strlen("lightning_board_")) == 0) {
        auto obj = params["message"].GetObject();

        bitflyer_board(db, line_timestamp, channel, obj);
    } else if (strncmp(channel, "lightning_ticker_", strlen("lightning_ticker_")) == 0) {
        auto obj = params["message"].GetObject();

        bitflyer_ticker(db, line_timestamp, channel, obj);
    } else {
        std::cerr << "unknown channel prefix: " << channel << std::endl;
        exit(1);
    }
}
