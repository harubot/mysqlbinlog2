What is this?
==================
Simple MySQL binlog CLI tool.

	$ mysqlbinlog2 /var/lib/mysql/mysql-bin.000001


Installation (on Linux environment)
==================

	$ sudo yum install git gcc-c++
	$ git clone https://github.com/qoosky/mysqlbinlog2.git
	$ cd mysqlbinlog2
	$ tar zxvf poco-1.6.0.tar.gz
	$ cd poco-1.6.0
	$ ./configure
	Configured for Linux
	$ make
	$ sudo make install
	$ cd ..
	$ make
	$ chmod +x mysqlbinlog2
	$ sudo mv mysqlbinlog2 /usr/local/bin/
	$ export PATH=/usr/local/bin:$PATH
	$ export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

Now, you can see the human readable MySQL log.

	$ mysqlbinlog2 ./mysql-bin.000001
	
	server_version,server_id
	5.5.44-log,1
	2015/06/12 14:43:03 UTC QUERY_EVENT             SET PASSWORD FOR 'root'@'localhost'=''
	2015/06/12 14:43:46 UTC QUERY_EVENT     mydb    create database mydb
	2015/06/12 14:44:07 UTC QUERY_EVENT     mydb    create table my_table( f1 integer, v2 varchar(8))
	2015/06/12 14:44:17 UTC QUERY_EVENT     mydb    BEGIN
	2015/06/12 14:44:17 UTC TABLE_MAP_EVENT mydb    my_table        col:2   id:33
	2015/06/12 14:44:17 UTC WRITE_ROWS_EVENT        mydb    my_table        1,varchar
	2015/06/12 14:44:17 UTC XID_EVENT
	2015/06/14 07:13:45 UTC QUERY_EVENT     mydb    BEGIN
	2015/06/14 07:13:45 UTC TABLE_MAP_EVENT mydb    my_table        col:2   id:33
	2015/06/14 07:13:45 UTC DELETE_ROWS_EVENT       mydb    my_table        1,varchar
	2015/06/14 07:13:45 UTC XID_EVENT
	2015/06/14 07:19:09 UTC QUERY_EVENT     mydb    BEGIN
	2015/06/14 07:19:09 UTC TABLE_MAP_EVENT mydb    my_table        col:2   id:33
	2015/06/14 07:19:09 UTC WRITE_ROWS_EVENT        mydb    my_table        1,varchar
	2015/06/14 07:19:09 UTC WRITE_ROWS_EVENT        mydb    my_table        2,varchar
	2015/06/14 07:19:09 UTC XID_EVENT
	2015/06/14 07:19:56 UTC QUERY_EVENT     mydb    BEGIN
	2015/06/14 07:19:56 UTC TABLE_MAP_EVENT mydb    my_table        col:2   id:33
	2015/06/14 07:19:56 UTC DELETE_ROWS_EVENT       mydb    my_table        1,varchar
	2015/06/14 07:19:56 UTC DELETE_ROWS_EVENT       mydb    my_table        2,varchar
	2015/06/14 07:19:56 UTC XID_EVENT
	2015/06/14 07:22:20 UTC QUERY_EVENT     mydb    BEGIN
	2015/06/14 07:22:20 UTC TABLE_MAP_EVENT mydb    my_table        col:2   id:33
	2015/06/14 07:22:20 UTC WRITE_ROWS_EVENT        mydb    my_table        3,varchar
	2015/06/14 07:22:20 UTC XID_EVENT
	2015/06/14 07:22:40 UTC QUERY_EVENT     mydb    BEGIN
	2015/06/14 07:22:40 UTC TABLE_MAP_EVENT mydb    my_table        col:2   id:33
	2015/06/14 07:22:40 UTC UPDATE_ROWS_EVENT       mydb    my_table        3,varchar => 4,varchar
	2015/06/14 07:22:40 UTC XID_EVENT
	2015/06/14 07:33:28 UTC QUERY_EVENT     mydb    BEGIN
	2015/06/14 07:33:28 UTC TABLE_MAP_EVENT mydb    my_table        col:2   id:33
	2015/06/14 07:33:28 UTC WRITE_ROWS_EVENT        mydb    my_table        5,varchar
	2015/06/14 07:33:28 UTC WRITE_ROWS_EVENT        mydb    my_table        6,varchar
	2015/06/14 07:33:28 UTC XID_EVENT
	2015/06/14 07:34:07 UTC QUERY_EVENT     mydb    BEGIN
	2015/06/14 07:34:07 UTC TABLE_MAP_EVENT mydb    my_table        col:2   id:33
	2015/06/14 07:34:07 UTC UPDATE_ROWS_EVENT       mydb    my_table        5,varchar => 99,varchar
	2015/06/14 07:34:07 UTC UPDATE_ROWS_EVENT       mydb    my_table        6,varchar => 99,varchar
	2015/06/14 07:34:07 UTC XID_EVENT


References
==================
- https://www.qoosky.dev/techs/2249ec5512
- https://www.qoosky.dev/techs/8e92d3d34a
- https://www.qoosky.dev/techs/5a407cd8cb
- https://www.qoosky.dev/techs/b8271736f9
- https://www.qoosky.dev/techs/b69c78a44d
- https://www.qoosky.dev/techs/736a22850b
- https://www.qoosky.dev/techs/b32397ba91
