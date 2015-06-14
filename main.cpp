#include "mysqlbinlog.h"
#include <Poco/DateTime.h>
#include <Poco/Timestamp.h>
#include <iostream>
#include <cstdlib>
using namespace std;

void usage() {
    cerr << "usage: mysqlbinlog2 mysql-bin.000001" << endl;
}

void printBinlogInfo(const MySQLBinlog& parser) {
    cout << "server_version,server_id" << endl;
    cout << parser.getServerVersion() << ','
         << parser.getServerId() << endl;
}

void printTimestamp(int time) {
    Poco::Timestamp epoch = Poco::Timestamp::fromEpochTime(time);
    Poco::DateTime dt(epoch);
    printf("%d/%02d/%02d %02d:%02d:%02d UTC", dt.year(), dt.month(), dt.day(), dt.hour(), dt.minute(), dt.second());
}

void printQueryEvent(const Event* event) {
    const string dbname = event->getDBName();
    const string sql_statement = event->getSQLStatement();
    cout << '\t' << "QUERY_EVENT" << '\t'
         << dbname << '\t'
         << sql_statement << endl;
}

void printStopEvent(const Event* event) {
    cout << '\t' << "STOP_EVENT" << endl;
}

void printRotateEvent(const Event* event) {
    cout << '\t' << "ROTATE_EVENT" << '\t'
         << event->getNextBinlogName() << endl;
}

void printXidEvent(const Event* event) {
    cout << '\t' << "XID_EVENT" << endl;
}

void storePrintTableMapEvent(const Event* event, TableMap& table_map, MetaMap& meta_map) {
    const int table_id = event->getTableId();
    const int num_of_columns = event->getNumOfColumns();
    const string dbname = event->getDBName();
    const string table_name = event->getTableName();
    cout << '\t' << "TABLE_MAP_EVENT" << '\t'
         << dbname << '\t'
         << table_name << '\t'
         << "col:" << num_of_columns << '\t'
         << "id:" << table_id << endl;
    table_map[table_id] = pss(dbname, table_name);

    vector<ColumnType> vc(num_of_columns);
    vector<int> vi(num_of_columns);
    for(int i = 0; i < num_of_columns; ++i) {
        vc[i] = event->getColumnType(i);
        vi[i] = event->getMetadata(i);
    }
    meta_map[table_id] = pvv(vc,vi);
}

void printWriteRowsEvent(const Event* event, const TableMap& table_map) {
    const int table_id = event->getTableId();
    const string database_name = table_map.at(table_id).first;
    const string table_name = table_map.at(table_id).second;

    vector<RowImg> rows = event->getRows();

    for(vector<RowImg>::iterator it = rows.begin(); it != rows.end(); ++it) {

        if(it != rows.begin())
            printTimestamp(event->getTimestamp());

        cout << '\t' << "WRITE_ROWS_EVENT" << '\t'
             << database_name << '\t'
             << table_name << '\t';

        for(RowImg::iterator cit = it->begin(); cit != it->end(); ++cit) {
            cout << *cit;
            if(cit + 1 != it->end()) cout << ',';
        }
        cout << endl;
    }
}

void printDeleteRowsEvent(const Event* event, const TableMap& table_map) {
    const int table_id = event->getTableId();
    const string database_name = table_map.at(table_id).first;
    const string table_name = table_map.at(table_id).second;

    vector<RowImg> rows = event->getRows();

    for(vector<RowImg>::iterator it = rows.begin(); it != rows.end(); ++it) {

        if(it != rows.begin())
            printTimestamp(event->getTimestamp());

        cout << '\t' << "DELETE_ROWS_EVENT" << '\t'
             << database_name << '\t'
             << table_name << '\t';

        for(RowImg::iterator cit = it->begin(); cit != it->end(); ++cit) {
            cout << *cit;
            if(cit + 1 != it->end()) cout << ',';
        }
        cout << endl;
    }
}

void printUpdateRowsEvent(const Event* event, const TableMap& table_map) {
    const int table_id = event->getTableId();
    const string database_name = table_map.at(table_id).first;
    const string table_name = table_map.at(table_id).second;

    vector<RowImg> rows = event->getRows();

    bool before_row = true;

    for(vector<RowImg>::iterator it = rows.begin(); it != rows.end(); ++it, before_row = !before_row) {

        if(before_row) {
            if(it != rows.begin())
                printTimestamp(event->getTimestamp());

            cout << '\t' << "UPDATE_ROWS_EVENT" << '\t'
                 << database_name << '\t'
                 << table_name << '\t';
        }

        for(RowImg::iterator cit = it->begin(); cit != it->end(); ++cit) {
            cout << *cit;
            if(cit + 1 != it->end()) cout << ',';
        }

        if(before_row) cout << " => ";
        else cout << endl;
    }
}

int main(int argc, const char* argv[]) {

    if (argc != 2) {
        usage();
        return EXIT_FAILURE;
    }

    const char* SRC_FILE = argv[1];

    MySQLBinlog parser;

    if(!parser.open(SRC_FILE)) {
        cerr << "file open failed " << SRC_FILE << endl;
        return EXIT_FAILURE;
    }

    printBinlogInfo(parser);

    TableMap table_map;
    MetaMap meta_map;

    while(parser.read()) {
        const Event* event = parser.getEvent(table_map, meta_map);
        const TypeCode type = event->getTypeCode();

        printTimestamp(event->getTimestamp());

        if (QUERY_EVENT == type) {
            printQueryEvent(event);
        }

        else if (STOP_EVENT == type) {
            printStopEvent(event);
        }

        else if (ROTATE_EVENT == type) {
            printRotateEvent(event);
        }

        else if (XID_EVENT == type) {
            printXidEvent(event);
        }

        else if (TABLE_MAP_EVENT == type) {
            storePrintTableMapEvent(event, table_map, meta_map);
        }

        else if (WRITE_ROWS_EVENT == type) {
            printWriteRowsEvent(event, table_map);
        }

        else if (UPDATE_ROWS_EVENT == type) {
            printUpdateRowsEvent(event, table_map);
        }

        else if (DELETE_ROWS_EVENT == type) {
            printDeleteRowsEvent(event, table_map);
        }

        delete event;
    }

    parser.close();

    return EXIT_SUCCESS;
}
