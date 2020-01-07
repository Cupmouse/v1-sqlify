#ifndef BITFLYER_H
#define BITFLYER_H

#include <sqlite3.h>
#include <rapidjson/document.h>

void bitflyer_emit(sqlite3 *db, unsigned long long line_timestamp, rapidjson::Document &doc);

void bitflyer_msg(sqlite3 *db, unsigned long long line_timestamp, rapidjson::Document &doc);

#endif
