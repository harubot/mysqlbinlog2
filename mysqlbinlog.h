#ifndef MYSQLBINLOG_H_201506132102
#define MYSQLBINLOG_H_201506132102

#include <fstream>
#include <string>
#include <map>
#include <utility>
#include <vector>

enum TypeCode {
    FORMAT_DESCRIPTION_EVENT=15,
    QUERY_EVENT=2,
    STOP_EVENT=3,
    ROTATE_EVENT=4,
    XID_EVENT=16,
    TABLE_MAP_EVENT=19,
    WRITE_ROWS_EVENT=23,
    UPDATE_ROWS_EVENT=24,
    DELETE_ROWS_EVENT=25,
};

enum ColumnType {
    MYSQL_TYPE_DECIMAL, MYSQL_TYPE_TINY,
    MYSQL_TYPE_SHORT, MYSQL_TYPE_LONG,
    MYSQL_TYPE_FLOAT, MYSQL_TYPE_DOUBLE,
    MYSQL_TYPE_NULL, MYSQL_TYPE_TIMESTAMP,
    MYSQL_TYPE_LONGLONG, MYSQL_TYPE_INT24,
    MYSQL_TYPE_DATE, MYSQL_TYPE_TIME,
    MYSQL_TYPE_DATETIME, MYSQL_TYPE_YEAR,
    MYSQL_TYPE_NEWDATE, MYSQL_TYPE_VARCHAR,
    MYSQL_TYPE_BIT,
    MYSQL_TYPE_TIMESTAMP2,
    MYSQL_TYPE_DATETIME2,
    MYSQL_TYPE_TIME2,
    MYSQL_TYPE_NEWDECIMAL=246,
    MYSQL_TYPE_ENUM=247,
    MYSQL_TYPE_SET=248,
    MYSQL_TYPE_TINY_BLOB=249,
    MYSQL_TYPE_MEDIUM_BLOB=250,
    MYSQL_TYPE_LONG_BLOB=251,
    MYSQL_TYPE_BLOB=252,
    MYSQL_TYPE_VAR_STRING=253,
    MYSQL_TYPE_STRING=254,
    MYSQL_TYPE_GEOMETRY=255,
};

typedef std::pair<std::string,std::string> pss;
typedef std::map<int,pss> TableMap;
typedef std::pair<std::vector<ColumnType>,std::vector<int> > pvv;
typedef std::map<int,pvv> MetaMap;
typedef std::vector<std::string> RowImg;

class Event {
 public:
    Event(int timestamp, TypeCode type_code, const char* data, int data_size,
          const TableMap& table_map, const MetaMap& meta_map);
    ~Event();

 public:
    int getTimestamp() const {
        return m_timestamp;
    };
    TypeCode getTypeCode() const {
        return m_type_code;
    };

 public:
    std::string getDBName() const;
    std::string getSQLStatement() const;

 public:
    std::string getNextBinlogName() const;

 public:
    int getTableId() const {
        return m_table_id;
    };
    int getNumOfColumns() const {
        return m_num_of_columns;
    };
    std::string getTableName() const;

 public:
    ColumnType getColumnType(int column_index) const;
    int getMetadata(int column_index) const;

 public:
    const std::vector<RowImg>& getRows() const {
        return m_rows;
    };

 private:
    bool parseQueryEventData();
    bool parseRotateEventData();
    bool parseTableMapEventData();
    bool parseWriteRowsEventData(const TableMap& table_map, const MetaMap& meta_map);
    bool parseUpdateRowsEventData(const TableMap& table_map, const MetaMap& meta_map);
    bool parseDeleteRowsEventData(const TableMap& table_map, const MetaMap& meta_map);
    bool __parseRowsEventData(const TableMap& table_map, const MetaMap& meta_map, bool is_update = false);

 private:
    const int m_timestamp;
    const TypeCode m_type_code;

 private:
    char* m_data;
    const int m_data_size;

 private:
    char* m_dbname;
    int m_dbname_size;
    char* m_sql_statement;
    int m_sql_statement_size;

 private:
    char* m_next_binlog_name;
    int m_next_binlog_name_size;

 private:
    int m_table_id;
    char* m_table_name;
    int m_table_name_size;
    ColumnType* m_column_types;
    int m_num_of_columns;
    char* m_metadata_block;
    int m_metadata_block_size;

 private:
    static int GetColumnImageSize(ColumnType ctype, unsigned int meta, const char* data);

 private:
    std::vector<RowImg> m_rows;
};

class MySQLBinlog {
 public:
    MySQLBinlog();
    ~MySQLBinlog();

 public:
    bool open(const char* src);
    bool read();
    Event* getEvent(const TableMap& table_map, const MetaMap& meta_map);
    bool close();

 public:
    std::string getServerVersion() const;
    int getServerId() const;

 private:
    bool checkBinlog();

 private:
    bool readHeader();
    bool readData(TypeCode type);

 private:
    char* m_timestamp_bytes;
    char* m_type_code_bytes;
    char* m_server_id_bytes;
    char* m_event_length_bytes;
    char* m_next_position_bytes;

 private:
    char* m_binlog_format_version_bytes;
    char* m_server_version_bytes;
    char* m_header_length_bytes;

 private:
    std::fstream m_src;

 private:
    static const int TIMESTAMP_BYTE_SIZE = 4;
    static const int TYPE_CODE_BYTE_SIZE = 1;
    static const int SERVER_ID_BYTE_SIZE = 4;
    static const int EVENT_LENGTH_BYTE_SIZE = 4;
    static const int NEXT_POSITION_BYTE_SIZE = 4;
    static const int FLAGS_BYTE_SIZE = 2;

 private:
    static const int BINLOG_FORMAT_VERSION_BYTE_SIZE = 2;
    static const int SERVER_VERSION_BYTE_SIZE = 50;
    static const int HEADER_LENGTH_BYTE_SIZE = 1;

 private:
    char* m_data;
    int m_data_size;
};

#endif // #ifndef MYSQLBINLOG_H_201506132102
