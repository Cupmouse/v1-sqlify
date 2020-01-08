#ifndef BITFINEX_H
#define BITFINEX_H

#include <sqlite3.h>
#include <rapidjson/document.h>

void bitfinex_emit(sqlite3 *db, unsigned long long line_timestamp, rapidjson::Document &doc);

void bitfinex_msg(sqlite3 *db, unsigned long long line_timestamp, rapidjson::Document &doc);

#endif
