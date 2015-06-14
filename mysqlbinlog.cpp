#include "mysqlbinlog.h"
#include <iostream>
#include <sstream>
#include <cstdlib>
using namespace std;

int bytes2dec(const char *bytes, const int BYTE_SIZE) {
    char* _bytes = new char[BYTE_SIZE];
    copy(bytes, bytes + BYTE_SIZE, _bytes);
    int decsum = 0;
    for(int i = 0; i < BYTE_SIZE; ++i) {
        int d = (int)(unsigned char)(_bytes[i]);
        for(int j = 0; j < i; ++j) d *= 256;
        decsum += d;
    }
    delete[] _bytes;
    return decsum;
}

long long unsigned int unpack_packed_integer(const char* data) {
    if (sizeof(long long unsigned int) < 8) {
        cerr << "cannot handle packed integer" << endl;
        exit(EXIT_FAILURE);
    }
    long long unsigned int packed_integer = bytes2dec(data, 1);
    if (packed_integer == 252) {
        packed_integer = bytes2dec(data + 1, 2);
        if (packed_integer < 251) {
            cerr << "unpack error (type 252)" << endl;
            exit(EXIT_FAILURE);
        }
    }
    else if (packed_integer == 253) {
        packed_integer = bytes2dec(data + 1, 3);
        if (packed_integer < 0xFFFF) {
            cerr << "unpack error (type 253)" << endl;
            exit(EXIT_FAILURE);
        }
    }
    else if (packed_integer == 254) {
        packed_integer = bytes2dec(data + 1, 8);
        if (packed_integer < 0xFFFFFF) {
            cerr << "unpack error (type 254)" << endl;
            exit(EXIT_FAILURE);
        }
    }
    return packed_integer;
}

string int2str(long long unsigned int n) {
    stringstream ss;
    ss << n << flush;
    return ss.str();
}

//******************************
// BINLOG PARSER CLASS
//******************************

MySQLBinlog::MySQLBinlog() {
    const int HEADER_SIZE_OF_FORMAT_DESCRIPTION_EVENT = 19;
    m_timestamp_bytes = new char[TIMESTAMP_BYTE_SIZE];
    m_type_code_bytes = new char[TYPE_CODE_BYTE_SIZE];
    m_server_id_bytes = new char[SERVER_ID_BYTE_SIZE];
    m_event_length_bytes = new char[EVENT_LENGTH_BYTE_SIZE];
    m_next_position_bytes = new char[NEXT_POSITION_BYTE_SIZE];
    m_binlog_format_version_bytes = new char[BINLOG_FORMAT_VERSION_BYTE_SIZE];
    m_server_version_bytes = new char[SERVER_VERSION_BYTE_SIZE];
    m_header_length_bytes = new char[HEADER_LENGTH_BYTE_SIZE];
    m_header_length_bytes[0] = HEADER_SIZE_OF_FORMAT_DESCRIPTION_EVENT;
    m_data_size = 0;
    m_data = new char[m_data_size];
}

MySQLBinlog::~MySQLBinlog() {
    delete[] m_timestamp_bytes;
    delete[] m_type_code_bytes;
    delete[] m_server_id_bytes;
    delete[] m_event_length_bytes;
    delete[] m_next_position_bytes;
    delete[] m_binlog_format_version_bytes;
    delete[] m_server_version_bytes;
    delete[] m_header_length_bytes;
    delete[] m_data;
}

bool MySQLBinlog::open(const char *src_file) {
    m_src.open(src_file, ios::in | ios::binary);
    return m_src.is_open() && checkBinlog();
}

bool MySQLBinlog::checkBinlog() {
    const int MAGIC_BYTE_SIZE = 4;
    char buf[MAGIC_BYTE_SIZE];
    m_src.read(buf, sizeof buf);
    if(m_src.fail() || m_src.gcount() != MAGIC_BYTE_SIZE) {
        return false;
    }
    if(!(buf[0] == static_cast<char>(0xfe) &&
         buf[1] == static_cast<char>(0x62) &&
         buf[2] == static_cast<char>(0x69) &&
         buf[3] == static_cast<char>(0x6e))) {
        cerr << "input file is not mysql binlog" << endl;
        return false;
    }
    if(!(readHeader() && m_type_code_bytes[0] == FORMAT_DESCRIPTION_EVENT)) {
        cerr << "cannot read FORMAT_DESCRIPTION_EVENT" << endl;
        return false;
    }
    if(!(readData(FORMAT_DESCRIPTION_EVENT))) {
        cerr << "cannot read FORMAT_DESCRIPTION_EVENT (data part)" << endl;
        return false;
    }
    if(!(m_binlog_format_version_bytes[0] == static_cast<char>(0x04) &&
         m_binlog_format_version_bytes[1] == static_cast<char>(0x00))) {
        cerr << "unsupported binlog version (not v4)" << endl;
        return false;
    }
    return true;
}

std::string MySQLBinlog::getServerVersion() const {
    stringstream ss;
    ss << m_server_version_bytes;
    return ss.str();
}

int MySQLBinlog::getServerId() const {
    return bytes2dec(m_server_id_bytes, SERVER_ID_BYTE_SIZE);
}

bool MySQLBinlog::readData(TypeCode type) {
    if (type == FORMAT_DESCRIPTION_EVENT) {
        m_src.clear();
        m_src.read(m_binlog_format_version_bytes, BINLOG_FORMAT_VERSION_BYTE_SIZE);
        m_src.read(m_server_version_bytes, SERVER_VERSION_BYTE_SIZE);
        m_src.seekg(TIMESTAMP_BYTE_SIZE, ios::cur);
        m_src.read(m_header_length_bytes, HEADER_LENGTH_BYTE_SIZE);
    }
    else {
        const int _data_size = bytes2dec(m_event_length_bytes, EVENT_LENGTH_BYTE_SIZE) -
            bytes2dec(m_header_length_bytes, HEADER_LENGTH_BYTE_SIZE);
        if(_data_size > m_data_size) {
            delete[] m_data;
            m_data = new char[_data_size];
        }
        m_data_size = _data_size;
        m_src.read(m_data, m_data_size);
    }
    return !m_src.fail();
}

bool MySQLBinlog::readHeader() {
    m_src.clear();
    m_src.read(m_timestamp_bytes, TIMESTAMP_BYTE_SIZE);
    m_src.read(m_type_code_bytes, TYPE_CODE_BYTE_SIZE);
    m_src.read(m_server_id_bytes, SERVER_ID_BYTE_SIZE);
    m_src.read(m_event_length_bytes, EVENT_LENGTH_BYTE_SIZE);
    m_src.read(m_next_position_bytes, NEXT_POSITION_BYTE_SIZE);
    m_src.seekg(FLAGS_BYTE_SIZE, ios::cur);
    int remaining_header_byte_size = m_header_length_bytes[0] -
        (TIMESTAMP_BYTE_SIZE
         + TYPE_CODE_BYTE_SIZE
         + SERVER_ID_BYTE_SIZE
         + EVENT_LENGTH_BYTE_SIZE
         + NEXT_POSITION_BYTE_SIZE
         + FLAGS_BYTE_SIZE
         );
    m_src.seekg(remaining_header_byte_size, ios::cur);
    return !m_src.fail();
}

bool MySQLBinlog::read() {
    m_src.clear();
    m_src.seekg(bytes2dec(m_next_position_bytes, NEXT_POSITION_BYTE_SIZE));
    return readHeader() && readData(static_cast<TypeCode>(m_type_code_bytes[0]));
}

bool MySQLBinlog::close() {
    m_src.close();
    return !m_src.is_open();
}

Event* MySQLBinlog::getEvent(const TableMap& table_map, const MetaMap& meta_map) {
    const int timestamp = bytes2dec(m_timestamp_bytes, TIMESTAMP_BYTE_SIZE);
    const TypeCode type_code = static_cast<TypeCode>(bytes2dec(m_type_code_bytes, TYPE_CODE_BYTE_SIZE));
    return new Event(timestamp, type_code, m_data, m_data_size, table_map, meta_map);
}

//******************************
// BINLOG EVENT CLASS
//******************************

Event::Event(int timestamp, TypeCode type_code, const char* data, int data_size,
             const TableMap& table_map, const MetaMap& meta_map):
    m_timestamp(timestamp), m_type_code(type_code), m_data_size(data_size)
{
    m_data = new char[data_size];
    copy(data, data + data_size, m_data);

    m_dbname_size = 0;
    m_dbname = new char[m_dbname_size];
    m_sql_statement_size = 0;
    m_sql_statement = new char[m_sql_statement_size];
    m_next_binlog_name_size = 0;
    m_next_binlog_name = new char[m_next_binlog_name_size];
    m_table_name_size = 0;
    m_table_name = new char[m_table_name_size];

    m_num_of_columns = 0;
    m_column_types = new ColumnType[m_num_of_columns];
    m_metadata_block_size = 0;
    m_metadata_block = new char[m_metadata_block_size];

    m_rows.clear();

    if (QUERY_EVENT == type_code) parseQueryEventData() || cerr << "parse failed QUERY_EVENT" << endl;
    if (ROTATE_EVENT == type_code) parseRotateEventData() || cerr << "parse failed ROTATE_EVENT" << endl;
    if (TABLE_MAP_EVENT == type_code) parseTableMapEventData() || cerr << "parse failed TABLE_MAP_EVENT" << endl;
    if (WRITE_ROWS_EVENT == type_code) parseWriteRowsEventData(table_map, meta_map) || cerr << "parse failed WRITE_ROWS_EVENT" << endl;
    if (UPDATE_ROWS_EVENT == type_code) parseUpdateRowsEventData(table_map, meta_map) || cerr << "parse failed UPDATE_ROWS_EVENT" << endl;
    if (DELETE_ROWS_EVENT == type_code) parseDeleteRowsEventData(table_map, meta_map) || cerr << "parse failed DELETE_ROWS_EVENT" << endl;
}

Event::~Event() {
    delete[] m_data;
    delete[] m_dbname;
    delete[] m_sql_statement;
    delete[] m_next_binlog_name;
    delete[] m_table_name;
    delete[] m_column_types;
    delete[] m_metadata_block;
}

bool Event::parseQueryEventData() {
    int pos = 0;
    const int dbname_size = bytes2dec
        (m_data + (pos += 8), 1);
    const int status_variable_size = bytes2dec
        (m_data + (pos += 3), 2);
    pos += 2 + status_variable_size;
    char* dbname = new char[dbname_size + 1];
    for(int i = 0; i <= dbname_size; ++i) dbname[i] = m_data[pos++];
    if(dbname[dbname_size] != '\0') {
        cerr << "QUERY_EVENT default database name is not null terminated" << endl;
        return false;
    }
    const int sql_statement_size = m_data_size - pos;
    char* sql_statement = new char[sql_statement_size + 1];
    for(int i = 0; i < sql_statement_size; ++i) sql_statement[i] = m_data[pos++];
    sql_statement[sql_statement_size] = '\0';

    delete[] m_dbname;
    m_dbname = dbname;
    m_dbname_size = dbname_size;
    delete[] m_sql_statement;
    m_sql_statement = sql_statement;
    m_sql_statement_size = sql_statement_size;

    return true;
}

string Event::getDBName() const {
    stringstream ss;
    ss << m_dbname;
    return ss.str();
}

string Event::getSQLStatement() const {
    stringstream ss;
    ss << m_sql_statement;
    return ss.str();
}

string Event::getNextBinlogName() const {
    stringstream ss;
    ss << m_next_binlog_name;
    return ss.str();
}

string Event::getTableName() const {
    stringstream ss;
    ss << m_table_name;
    return ss.str();
}

bool Event::parseRotateEventData() {
    int pos = 8;
    const int next_binlog_name_size = m_data_size - pos;
    char* next_binlog_name = new char[next_binlog_name_size + 1];
    for(int i = 0; i < next_binlog_name_size; ++i) next_binlog_name[i] = m_data[pos++];
    next_binlog_name[next_binlog_name_size] = '\0';

    delete[] m_next_binlog_name;
    m_next_binlog_name = next_binlog_name;
    m_next_binlog_name_size = next_binlog_name_size;

    return true;
}

bool Event::parseTableMapEventData() {
    int pos = 0;
    const int table_id = bytes2dec(m_data, 6);
    const int database_name_size = bytes2dec
        (m_data + (pos += 8), 1);
    char* database_name = new char[database_name_size + 1];
    for(int i = 0; i <= database_name_size; ++i) database_name[i] = m_data[++pos];
    if(database_name[database_name_size] != '\0') {
        cerr << "TableMapEventData database name is not null terminated" << endl;
        return false;
    }
    const int table_name_size = bytes2dec
        (m_data + (pos += 1), 1);
    char* table_name = new char[table_name_size + 1];
    for(int i = 0; i <= table_name_size; ++i) table_name[i] = m_data[++pos];
    if(table_name[table_name_size] != '\0') {
        cerr << "TableMapEventData table name is not null terminated" << endl;
        return false;
    }
    const int num_of_columns = unpack_packed_integer(m_data + (pos += 1));
    ColumnType* column_types = new ColumnType[num_of_columns];
    for(int i = 0; i < num_of_columns; ++i) column_types[i] = static_cast<ColumnType>(m_data[++pos]);
    const long long unsigned int metadata_block_size = unpack_packed_integer(m_data + (pos += 1));
    int packed_integer_type = bytes2dec(m_data, 1);
    if (packed_integer_type < 252) pos += 1;
    else if (packed_integer_type == 252) pos += 3;
    else if (packed_integer_type == 253) pos += 4;
    else if (packed_integer_type == 254) pos += 9;
    char* metadata_block = new char[metadata_block_size];
    for(long long unsigned int i = 0; i < metadata_block_size; ++i) metadata_block[i] = m_data[pos++];

    m_table_id = table_id;
    m_num_of_columns = num_of_columns;

    delete[] m_dbname;
    m_dbname = database_name;
    m_dbname_size = database_name_size;

    delete[] m_table_name;
    m_table_name = table_name;
    m_table_name_size = table_name_size;

    delete[] m_column_types;
    m_column_types = column_types;

    delete[] m_metadata_block;
    m_metadata_block = metadata_block;
    m_metadata_block_size = metadata_block_size;

    return true;
}

ColumnType Event::getColumnType(int column_index) const {
    return m_column_types[column_index];
}

int Event::getMetadata(int column_index) const {
    int pos = 0;
    int metadata_size;
    for(int i = 0;; ++i) {
        ColumnType ctype = m_column_types[i];
        switch(ctype) {
            case MYSQL_TYPE_FLOAT:
            case MYSQL_TYPE_DOUBLE:
            case MYSQL_TYPE_BLOB:
            case MYSQL_TYPE_GEOMETRY:
                metadata_size = 1;
                break;
            case MYSQL_TYPE_VARCHAR:
            case MYSQL_TYPE_BIT:
            case MYSQL_TYPE_NEWDECIMAL:
            case MYSQL_TYPE_VAR_STRING:
            case MYSQL_TYPE_STRING:
                metadata_size = 2;
                break;
            default:
                metadata_size = 0;
                break;
        }
        if (i == column_index) break;
        pos += metadata_size;
    }
    if(pos + metadata_size > m_metadata_block_size) {
        cerr << "invalid metadata access" << endl;
        exit(EXIT_FAILURE);
    }
    return bytes2dec(m_metadata_block + pos, metadata_size);
}

int Event::GetColumnImageSize(ColumnType ctype, unsigned int meta, const char* data) {
    int csize = 0;
    switch(ctype) {
        case MYSQL_TYPE_TINY:
        case MYSQL_TYPE_YEAR:
            csize = 1;
            break;
        case MYSQL_TYPE_SHORT:
            csize = 2;
            break;
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_TIME:
        case MYSQL_TYPE_NEWDATE:
            csize = 3;
            break;
        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_FLOAT:
        case MYSQL_TYPE_TIMESTAMP:
            csize = 4;
            break;
        case MYSQL_TYPE_LONGLONG:
        case MYSQL_TYPE_DOUBLE:
        case MYSQL_TYPE_DATETIME:
            csize = 8;
            break;
        default:
            break;
    }

    if (MYSQL_TYPE_VARCHAR == ctype ||
        MYSQL_TYPE_VAR_STRING == ctype) {
        csize = (int)(unsigned char)(data[0]) + (meta < 256 ? 1 : 2);
    }
    else if (MYSQL_TYPE_STRING == ctype) {
        if (meta >= 256) {
            unsigned int byte0= meta >> 8;
            unsigned int byte1= meta & 0xFF;
            if ((byte0 & 0x30) != 0x30) {
                meta = byte1 | (((byte0 & 0x30) ^ 0x30) << 4);
            }
            else {
                meta = meta & 0xFF;
            }
        }
        csize = (int)(unsigned char)(data[0]) + (meta < 256 ? 1 : 2);
    }
    else if (MYSQL_TYPE_BIT == ctype) {
        unsigned int nbits = ((meta >> 8) * 8) + (meta & 0xFF);
        csize = (nbits + 7) / 8;
    }
    else if (MYSQL_TYPE_ENUM == ctype) {
        switch (meta & 0xFF) {
            case 1:
                csize = 1;
                break;
            case 2:
                csize = 2;
                break;
            default:
                csize = 0;
                break;
        }
    }
    else if (MYSQL_TYPE_SET == ctype) {
        csize = meta & 0xFF;
    }
    else if(MYSQL_TYPE_TIME2 == ctype) {
        csize = 3 + (meta + 1) / 2;
    }
    else if (MYSQL_TYPE_TIMESTAMP2 == ctype) {
        csize = 4 + (meta + 1) / 2;
    }
    else if (MYSQL_TYPE_DATETIME2 == ctype) {
        csize = 5 + (meta + 1) / 2;
    }
    else if (MYSQL_TYPE_BLOB == ctype) {
        csize = (int)(unsigned char)(data[0]) + meta;
    }
    else if (MYSQL_TYPE_NEWDECIMAL == ctype) {
        // TODO
        // unsigned int precision = meta >> 8;
        // unsigned int decimals = meta & 0xFF;
        // csize = decimal_bin_size((int)precision, (int)scale);
        cerr << "do not know how to parse MYSQL_TYPE_NEWDECIMAL" << endl;
        exit(EXIT_FAILURE);
    }
    return csize;
}

bool Event::parseWriteRowsEventData(const TableMap& table_map, const MetaMap& meta_map) {
    return __parseRowsEventData(table_map, meta_map);
}

bool Event::parseUpdateRowsEventData(const TableMap& table_map, const MetaMap& meta_map) {
    return __parseRowsEventData(table_map, meta_map, true);
}

bool Event::parseDeleteRowsEventData(const TableMap& table_map, const MetaMap& meta_map) {
    return __parseRowsEventData(table_map, meta_map);
}

bool Event::__parseRowsEventData(const TableMap& table_map, const MetaMap& meta_map, bool is_update) {

    int pos = 0;
    const int table_id = bytes2dec(m_data, 6);
    const int num_of_columns = unpack_packed_integer(m_data + (pos += 8));

    bool* used_column = new bool[num_of_columns];
    bool* null_column = new bool[num_of_columns];

    int mask_byte_size = (num_of_columns + 7) / 8;

    if(is_update) pos += mask_byte_size * 2;
    else pos += mask_byte_size;

    for(int i = 0; i < mask_byte_size; ++i) {
        unsigned char byte = m_data[pos++];
        for(int j = 0; j < 8; ++j) {
            int ii = (i * 8) + j;
            if (ii >= num_of_columns) break;
            used_column[ii] = byte & (1 << j);
        }
    }

    int remaining_byte_size = m_data_size - pos;

    while(remaining_byte_size > 0) {

        int num_of_used_columns = 0;
        for(int i = 0; i < num_of_columns; ++i)
            if(used_column[i]) ++num_of_used_columns;

        mask_byte_size = (num_of_used_columns + 7) / 8;

        for(int i = 0, offset = 0; i < mask_byte_size; ++i) {
            unsigned char byte = m_data[pos++];
            for(int j = 0; j < 8; ++j) {
                int ii = (i * 8) + j + offset;
                while(ii < num_of_columns && !used_column[ii]) {
                    ++offset;
                    ++ii;
                }
                if (ii >= num_of_columns) break;
                null_column[ii] = byte & (1 << j);
            }
        }

        remaining_byte_size -= mask_byte_size;

        vector<string> row(num_of_columns);

        for(int i = 0; i < num_of_columns; ++i) {

            if(!used_column[i]) {
                row[i] = "-";
                continue;
            }
            else if(null_column[i]) {
                row[i] = "null";
                continue;
            }

            const ColumnType ctype = meta_map.at(table_id).first.at(i);
            const unsigned int meta = static_cast<unsigned int>(meta_map.at(table_id).second.at(i));
            const int csize = GetColumnImageSize(ctype, meta, m_data + pos);

            if(ctype ==MYSQL_TYPE_DECIMAL) row[i] = "decimal";
            else if(ctype == MYSQL_TYPE_TINY) row[i] = int2str(bytes2dec(m_data + pos, csize));
            else if(ctype == MYSQL_TYPE_SHORT) row[i] = int2str(bytes2dec(m_data + pos, csize));
            else if(ctype == MYSQL_TYPE_LONG) row[i] = int2str(bytes2dec(m_data + pos, csize));
            else if(ctype == MYSQL_TYPE_FLOAT) row[i] = "float";
            else if(ctype == MYSQL_TYPE_DOUBLE) row[i] = "double";
            else if(ctype == MYSQL_TYPE_NULL) row[i] = "null";
            else if(ctype == MYSQL_TYPE_TIMESTAMP) row[i] = "timestamp";
            else if(ctype == MYSQL_TYPE_LONGLONG) row[i] = "longlong";
            else if(ctype == MYSQL_TYPE_INT24) row[i] = int2str(bytes2dec(m_data + pos, csize));
            else if(ctype == MYSQL_TYPE_DATE) row[i] = "date";
            else if(ctype == MYSQL_TYPE_TIME) row[i] = "time";
            else if(ctype == MYSQL_TYPE_DATETIME) row[i] = "datetime";
            else if(ctype == MYSQL_TYPE_YEAR) row[i] = "year";
            else if(ctype == MYSQL_TYPE_NEWDATE) row[i] = "newdate";
            else if(ctype == MYSQL_TYPE_VARCHAR) row[i] = "varchar";
            else if(ctype == MYSQL_TYPE_BIT) row[i] = "bit";
            else if(ctype == MYSQL_TYPE_TIMESTAMP2) row[i] = "timestamp2";
            else if(ctype == MYSQL_TYPE_DATETIME2) row[i] = "datetime2";
            else if(ctype == MYSQL_TYPE_TIME2) row[i] = "time2";
            else if(ctype == MYSQL_TYPE_NEWDECIMAL) row[i] = "newdecimal";
            else if(ctype == MYSQL_TYPE_ENUM) row[i] = "enum";
            else if(ctype == MYSQL_TYPE_SET) row[i] = "set";
            else if(ctype == MYSQL_TYPE_TINY_BLOB) row[i] = "tiny_blob";
            else if(ctype == MYSQL_TYPE_MEDIUM_BLOB) row[i] = "medium_blob";
            else if(ctype == MYSQL_TYPE_LONG_BLOB) row[i] = "long_blob";
            else if(ctype == MYSQL_TYPE_BLOB) row[i] = "blob";
            else if(ctype == MYSQL_TYPE_VAR_STRING) row[i] = "var_string";
            else if(ctype == MYSQL_TYPE_STRING) row[i] = "string";
            else if(ctype == MYSQL_TYPE_GEOMETRY) row[i] = "geometry";
            else  row[i] = "unknown";

            pos += csize;
            remaining_byte_size -= csize;
        }
        m_rows.push_back(row);
    }

    m_table_id = table_id;

    delete[] null_column;
    delete[] used_column;

    return true;
}
