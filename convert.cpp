#include <ctime>
#include <iostream>
#include <sqlite3.h>
#include <rapidjson/document.h>

#include "common.h"
#include "bitflyer.h"

using namespace rapidjson;

inline unsigned long long timestamp_nanosec(char *str) {
    struct tm time;
    unsigned long long nanosec;

    memset(&time, 0, sizeof(struct tm));

    time.tm_isdst = -1;
    strptime(str, "%Y-%m-%d %H:%M:%S", &time);

    nanosec = ((unsigned long long) timegm(&time)) * 1000000000;
    nanosec += atol(str+strlen("2020-01-01 19:12:03.")) * 100;

    return nanosec;
}

inline sqlite3 *connect_database(char *filename) {
    sqlite3 *db;
    int r;

    // open database with read and write, create file if not exist
    r = sqlite3_open_v2(filename, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
    
    if (r != SQLITE_OK) {
        std::cerr << "sqlite error: " << sqlite3_errmsg(db) << std::endl;
        exit(1);
    }

    return db;
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        std::cerr << "argc == 3" << std::endl;
        exit(1);
    }

    // open database
    int r;
    char *err;
    sqlite3 *db = connect_database(argv[1]);

    
    /* start reading */
    // buffer for storing an line
    char* buf = (char*) std::malloc(sizeof(char)*N_L);
    // initialize buffer
    memset(buf, 0, N_L);
    // json parser
    Document doc;
    unsigned long long line_timestamp;
    unsigned long long num_line = 0;

    // skip head
    std::cin.getline(buf, N_L);

    // setup new transaction
    r = sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, &err);

    if (r != SQLITE_OK) {
        std::cerr << "starting new transaction failed: " << err << std::endl;
        sqlite3_free(err);
        exit(1);
    }

    while (std::cin.getline(buf, N_L, ',')) {
        if (buf[0] == 'm' && buf[1] == 's' && buf[2] == 'g') {
            // read timestamp
            std::cin.getline(buf, N_L, ',');
            line_timestamp = timestamp_nanosec(buf);

            // rest of the line is a msg
            std::cin.getline(buf, N_L);
            // std::cout << buf << std::endl;
            // setting kParseFullPrecisionFlag to obitain price and size in full precision
            doc.Parse<kParseFullPrecisionFlag>(buf);

            if (strcmp(argv[2], "bitfinex") == 0) {

            } else if (strcmp(argv[2], "bitmex") == 0) {

            } else if (strcmp(argv[2], "bitflyer") == 0) {
                bitflyer_msg(db, line_timestamp, doc);
            } else {
                std::cerr << "unknown exchange name" << std::endl;
                exit(1);
            }
        } else if (buf[0] == 'e' && buf[1] == 'm' && buf[2] == 'i' && buf[3] == 't') {
            std::cin.getline(buf, N_L, ',');
            line_timestamp = timestamp_nanosec(buf);

            std::cin.getline(buf, N_L);
            doc.Parse<kParseFullPrecisionFlag>(buf);
            
            if (strcmp(argv[2], "bitfinex") == 0) {

            } else if (strcmp(argv[2], "bitmex") == 0) {

            } else if (strcmp(argv[2], "bitflyer") == 0) {
                bitflyer_emit(db, line_timestamp, doc);
            } else {
                std::cerr << "unknown exchange name" << std::endl;
                exit(1);
            }
        } else {
            std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
        }

        // expect a next line
        num_line++;

        if (num_line % 100000 == 0) {
            // commit before and start a new transaction
            r = sqlite3_exec(db, "COMMIT", NULL, NULL, &err);

            if (r != SQLITE_OK) {
                std::cerr << "commit failed: " << err << std::endl;
                sqlite3_free(err);
                exit(1);
            }

            // start a new transaction
            r = sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, &err);

            if (r != SQLITE_OK) {
                std::cerr << "starting new transaction failed: " << err << std::endl;
                sqlite3_free(err);
                exit(1);
            }
        }
    }

    // commit all
    r = sqlite3_exec(db, "COMMIT", NULL, NULL, &err);

    if (r != SQLITE_OK) {
        std::cerr << "commit failed: " << err << std::endl;
        sqlite3_free(err);
        exit(1);
    }

    sqlite3_close_v2(db);

    return 0;
}