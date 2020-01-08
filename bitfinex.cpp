#include <iostream>
#include <map>
#include <sqlite3.h>
#include <rapidjson/document.h>

#include "common.h"

using namespace rapidjson;

// stores websocket channel id vs channel name
std::map<int, char *> chanIds;

inline void bitfinex_book_single(sqlite3 *db,
    unsigned long long line_timestamp,
    char *channel,
    rapidjson::GenericArray<false, rapidjson::Value::ValueType> array) {
    
    double price = array[0].GetDouble();
    // unsigned int count = array[1].GetUint();
    // negative if ask
    double amount = array[2].GetDouble();

    // insert into table corresponding to the channel name
    char *sql = (char *) malloc(sizeof(char)*N_SQL);

    // construct a sql statement
    snprintf(sql, N_SQL, "INSERT INTO %s VALUES(%llu, %.10f, %.10f)", channel, line_timestamp, price, amount);

    execute_insert(db, sql);

    free(sql);
}

inline void bitfinex_book(sqlite3 *db,
    unsigned long long line_timestamp,
    char *channel,
    rapidjson::GenericArray<false, rapidjson::Value::ValueType> array) {

    if (array[0].IsArray()) {
        // it is the first message, and its getting the full orderbook
        for (auto i = array.begin(); i != array.end(); i++) {
            bitfinex_book_single(db, line_timestamp, channel, i->GetArray());
        }
    } else {
        // single orderbook update
        bitfinex_book_single(db, line_timestamp, channel, array);
    }
}

inline void bitfinex_trades(sqlite3 *db,
    char *channel,
    rapidjson::GenericArray<false, rapidjson::Value::ValueType> array) {
 
    // unsigned int tradeId = doc[0].GetUint();
    // millisec timestamp
    unsigned long long timestamp = array[1].GetUint64();
    // convert it to nanosec timestamp
    timestamp *= 1000;
    // negative if sell
    double amount = array[2].GetDouble();
    double price = array[3].GetDouble();

    // insert into table corresponding to the channel name
    char *sql = (char *) malloc(sizeof(char)*N_SQL);

    // construct a sql statement
    snprintf(sql, N_SQL, "INSERT INTO %s VALUES(%llu, %.10f, %.10f)", channel, timestamp, price, amount);

    execute_insert(db, sql);

    free(sql);
}

void bitfinex_emit(sqlite3 *db,
    unsigned long long line_timestamp,
    rapidjson::Document &doc) {

    // nothing to do, ignore
}

void bitfinex_msg(sqlite3 *db,
    unsigned long long line_timestamp,
    rapidjson::Document &doc) {

    if (doc.IsObject()) {
        const char *event = doc["event"].GetString();

        if (strcmp(event, "subscribed") == 0) {
            // a response to subscription request
            const char *event_channel = doc["channel"].GetString();
            const char *symbol = doc["symbol"].GetString();
            unsigned int chanId = doc["chanId"].GetUint();

            char *channel = (char *) malloc(sizeof(char)*N_PAIR);
            snprintf(channel, N_PAIR, "%s_%s", event_channel, symbol);

            chanIds[chanId] = channel;

            TableType tt;
            if (strcmp(event_channel, "trades")) {
                tt = Trade;

            } else if (strcmp(event_channel, "book")) {
                tt = Book;

            } else {
                std::cerr << "unknown event channel name: " << event_channel << std::endl;
                exit(1);
            }

            create_new_table(db, tt, channel);

        } else if (strcmp(event, "info") == 0) {
            // ignore infomation event
            return;
        } else {
            std::cerr << "unknown event name: " << event << std::endl;
            return;
        }
    } else {
        // must be an array

        unsigned int chanId = doc[0].GetUint();

        char *channel = chanIds[chanId];

        if (strncmp(channel, "trades", strlen("trades")) == 0) {
            if (doc[1].IsArray()) {
                // if this is array, this must be the first message be get from this channel
                // i don't know what is this message, but it's an array of recent trades?
                // ignore
                return;
            }

            const char *type = doc[1].GetString();

            if (type[0] == 't' && type[1] == 'e') {
                auto array = doc[2].GetArray();

                // trade execution
                bitfinex_trades(db, channel, array);
            } else if (type[0] == 't' && type[1] == 'u') {
                // trade update, ignore
                return;
            } else if (type[0] == 'h' && type[1] == 'b') {
                // FIXME what is hb?
                return;
            } else {
                std::cerr << "unknown type: " << type << std::endl;
                exit(1);
            }

        } else if (strncmp(channel, "book", strlen("book")) == 0) {
            if (doc[1].IsString()) {
                // has type
                const char *type = doc[1].GetString();

                if (type[0] == 'h' && type[1] == 'b') {
                    // FIXME i have no idea what is hb
                    return;
                } else {
                    std::cerr << "unknown type: " << type << std::endl;
                    exit(1);
                }
            } else {
                auto array = doc[1].GetArray();

                bitfinex_book(db, line_timestamp, channel, array);
            }
        } else {
            std::cerr << "unknown channel prefix: " << channel << std::endl;
            exit(1);
        }
    }
}
