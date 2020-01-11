#ifndef BITMEX_H
#define BITMEX_H

#include <sqlite3.h>
#include <rapidjson/document.h>

void bitmex_emit(sqlite3 *db, unsigned long long line_timestamp, rapidjson::Document &doc);

void bitmex_msg(sqlite3 *db, unsigned long long line_timestamp, rapidjson::Document &doc);

#endif
