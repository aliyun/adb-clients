/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.alibaba.cloud.analyticdb.adbclient;

import com.alibaba.druid.pool.DruidDataSource;

import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import com.alibaba.druid.pool.DruidPooledPreparedStatement;
import com.mysql.jdbc.JDBC4PreparedStatement;
import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;


/**
 * AnalyticDB client sdk <br>
 *
 * @author chase
 */
public class AdbClient {
    private DatabaseConfig databaseConfig;
    private DruidDataSource dataSource;
    private static Map<String, DruidDataSource> dataSourceMap = new ConcurrentHashMap<String, DruidDataSource>();
    private Map<String, TableInfo> tableInfo;
    private Map<String, Integer> partitionColumnIndex;
    private Map<String, Boolean> isAllColumn;
    /**
     * Map< tableName, Map< columnName, Pair < java sql type, ads type name > > >
     */
    private Map<String, Map<String, Pair<Integer, String>>> tableColumnsMetaData;
    private Map<String, Map<String, Pair<Integer, String>>> configColumnsMetaData;

    private Map<String, String> insertSqlPrefix = new HashMap<String, String>();

    private static final String INSERT_TEMPLATE = "insert into %s ( %s ) values ";
    private static final String INSERT_ALL_COLUMN_TEMPLATE = "insert into %s values ";
    private static final String INSERT_IGNORE_TEMPLATE = "insert ignore into %s ( %s ) values ";
    private static final String INSERT_IGNORE_ALL_COLUMN_TEMPLATE = "insert ignore into %s values ";
    private static final String COLUMN_QUOTE_CHARACTER = "`";
    private static final String ALL_COLUMN_CHARACTER = "*";
    private static final String SQL_SPLIT_CHARACTER = " ,  ";
    private static final int SQL_SPLIT_CHARACTER_LEN = 4;
    protected static final long DEFAULT_SQL_LENGTH_LIMIT = 32 * 1024;

    private AtomicInteger exceptionCount = new AtomicInteger(0);
    private AtomicLong totalCount = new AtomicLong(0);
    private long periodStartTime = 0L;
    private long periodTime = 60 * 1000L;
    private Map<String, Map<Integer, MutablePair<StringBuilder, Integer>>> partitionBatch = new HashMap<String, Map<Integer, MutablePair<StringBuilder, Integer>>>();
    private List<String> commitExceptionDataList = Collections.synchronizedList(new ArrayList<String>());
    private MySQLSyntaxErrorException commitException = null;

    private final ExecutorService executorService;
    private Boolean isSync = false;
    private LinkedBlockingQueue<StringBuilder> sqlQueue = new LinkedBlockingQueue<StringBuilder>();

    private Map<String, StringBuilder> batchBuffer;
    private ClientDataSource clientDataSource;

    public AdbClient(DatabaseConfig databaseConfig) {
        this.databaseConfig = databaseConfig;
        periodStartTime = System.currentTimeMillis();
        clientDataSource = ClientDataSource.getInstance();
        initDatasource();
        this.executorService = new ThreadPoolExecutor(databaseConfig.getParallelNumber(),
                databaseConfig.getParallelNumber(),
                0,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new AdbClientThreadFactory(String.format("%s", databaseConfig.getTable())));
        initInstance();
    }

    public AdbClient(DatabaseConfig databaseConfig, Boolean isSync) {
        this.databaseConfig = databaseConfig;
        periodStartTime = System.currentTimeMillis();
        clientDataSource = ClientDataSource.getInstance();
        initDatasource();
        this.executorService = null;
        this.isSync = isSync;
        initInstance();
    }

    /**
     * 支持外部传入DruidDataSource
     *
     * @param databaseConfig DatabaseConfig
     * @param dataSource     DruidDataSource
     */
    public AdbClient(DatabaseConfig databaseConfig, DruidDataSource dataSource) {
        this.dataSource = dataSource;
        this.databaseConfig = databaseConfig;
        clientDataSource = ClientDataSource.getInstance();
        periodStartTime = System.currentTimeMillis();
        initDatasource();
        this.executorService = new ThreadPoolExecutor(databaseConfig.getParallelNumber(),
                databaseConfig.getParallelNumber(),
                0,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new AdbClientThreadFactory(String.format("%s", databaseConfig.getTable())));
        initInstance();
    }

    /**
     * 支持外部传入DruidDataSource和ExecutorService
     *
     * @param databaseConfig DatabaseConfig
     * @param dataSource     DruidDataSource
     * @param isSync         Boolean
     */
    public AdbClient(DatabaseConfig databaseConfig, DruidDataSource dataSource, Boolean isSync) {
        this.dataSource = dataSource;
        this.databaseConfig = databaseConfig;
        clientDataSource = ClientDataSource.getInstance();
        periodStartTime = System.currentTimeMillis();
        initDatasource();
        this.executorService = null;
        this.isSync = isSync;
        initInstance();
    }

    /**
     * Init client instance
     *
     * @return
     */
    private Boolean initInstance() {
        if (partitionBatch.size() > 0) {
            return false;
        }
        this.tableInfo = new HashMap<String, TableInfo>();
        this.partitionColumnIndex = new HashMap<String, Integer>();
        this.isAllColumn = new HashMap<String, Boolean>();
        this.configColumnsMetaData = new HashMap<String, Map<String, Pair<Integer, String>>>();
        this.tableColumnsMetaData = new HashMap<String, Map<String, Pair<Integer, String>>>();
        for (String tableName : databaseConfig.getTable()) {
            isAllColumn.put(tableName, false);
            partitionColumnIndex.put(tableName, -1);
        }
        if (!databaseConfig.isPartitionBatch()) {
            batchBuffer = new HashMap<String, StringBuilder>();
        }
        checkConfig();
        if (databaseConfig.getParallelNumber() <= 0) {
            databaseConfig.setParallelNumber(1);
        }
        logger("info", "init adb client successfully");
        return true;
    }

    private void initNewTable(String tableName) {
        try {
            getTableInfo(databaseConfig.getDatabase(), Collections.singletonList(tableName), this.getConnection());
        } catch (AdbClientException e) {
            throw e;
        } catch (Exception e) {
            throw new AdbClientException(AdbClientException.CONFIG_ERROR, "Init new table error:" + e.getMessage(), e);
        }
        if (this.tableInfo.get(tableName) == null) {
            throw new AdbClientException(AdbClientException.CONFIG_ERROR, "The table " + tableName + " do not exist", null);
        }
        if (!this.databaseConfig.getTable().contains(tableName)) {
            this.databaseConfig.setColumns(tableName, Collections.singletonList(ALL_COLUMN_CHARACTER));
            this.databaseConfig.getTable().add(tableName);
        }
        this.partitionColumnIndex.put(tableName, -1);
        checkTableConfig(tableName);
    }

    /**
     * Add Row data
     *
     * @param table String
     * @param row   Row
     */
    public void addRow(String table, Row row) {
        String tableName = table.toLowerCase();
        if (this.tableInfo.get(tableName) == null) {
            throw new AdbClientException(AdbClientException.CONFIG_ERROR, "The table " + tableName + " do not exist", null);
        }

        if (row.getColumnValues().size() != databaseConfig.getColumns(tableName).size()) {
            throw new AdbClientException(AdbClientException.ADD_DATA_ERROR, "Add row data is illegal, column size is not equal as config", null);
        }

        Connection conn = null;
        conn = this.getConnection();
        StringBuilder sqlSb = null;
        int partitionId = 0;
        // Write by partition
        if (databaseConfig.isPartitionBatch()) {
            partitionId = getHashPartition(tableName, row);
            if (partitionBatch.get(tableName) == null) {
                partitionBatch.put(tableName, new HashMap<Integer, MutablePair<StringBuilder, Integer>>());
            }
            if (partitionBatch.get(tableName).get(partitionId) == null) {
                partitionBatch.get(tableName).put(partitionId, new MutablePair<StringBuilder, Integer>(new StringBuilder(), 0));
            }
            sqlSb = partitionBatch.get(tableName).get(partitionId).getLeft();
        } else {
            // Single combine
            if (batchBuffer.get(tableName) == null) {
                batchBuffer.put(tableName, new StringBuilder());
            }
            sqlSb = batchBuffer.get(tableName);
        }
        try {
            String sqlResult = generateInsertSql(tableName, conn, row);
            closeDBResources(null, null, conn);
            String subSql = sqlResult.substring(insertSqlPrefix.get(tableName).length());
            int subSqlLen = subSql.getBytes().length;
            if (sqlSb.length() > 0) {
                int l = partitionBatch.get(tableName).get(partitionId).getRight() + subSqlLen;
                if (l + SQL_SPLIT_CHARACTER_LEN >= databaseConfig.getCommitSize()) {
                    try {
                        if (commitExceptionDataList.size() > 0 || commitException != null) {
                            commitExceptionDataList.clear();
                            commitException = null;
                        }
                        if (databaseConfig.isPartitionBatch()) {
                            // 自动提交只会提交单条语句
                            executeBatchSql(sqlSb);
                            if (partitionBatch.get(tableName) == null) {
                                partitionBatch.put(tableName, new HashMap<Integer, MutablePair<StringBuilder, Integer>>());
                            }
                            partitionBatch.get(tableName).put(partitionId, new MutablePair<StringBuilder, Integer>(new StringBuilder(), 0));
                            sqlSb = partitionBatch.get(tableName).get(partitionId).getLeft();
                        } else {
                            // Commit directly
                            executeBatchSql(sqlSb);
                            sqlSb = new StringBuilder();
                            batchBuffer.put(tableName, sqlSb);
                        }
                        if (commitExceptionDataList.size() > 0) {
                            logger("error", "Auto commit error data list " + commitExceptionDataList.toString());
                            throw new AdbClientException(AdbClientException.COMMIT_ERROR_DATA_LIST, commitExceptionDataList, commitException);
                        }
                    } catch (AdbClientException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new AdbClientException(AdbClientException.SQL_LENGTH_LIMIT, "Sql length is too large, auto commit failed. Please commit first. exception:" + e.getMessage(), e);
                    }
                }
            }
            if (sqlSb.length() == 0) {
                sqlSb.append(sqlResult.replace(SQL_SPLIT_CHARACTER, " , "));
                subSqlLen += insertSqlPrefix.get(tableName).getBytes().length;
            } else {
                sqlSb.append(SQL_SPLIT_CHARACTER);
                sqlSb.append(subSql.replace(SQL_SPLIT_CHARACTER, " , "));
                subSqlLen += SQL_SPLIT_CHARACTER_LEN;
            }
            partitionBatch.get(tableName).get(partitionId).setRight(partitionBatch.get(tableName).get(partitionId).getRight() + subSqlLen);
            totalCount.incrementAndGet();
        } catch (AdbClientException e) {
            logger("error", "addRow " + e.getMessage());
            throw e;
        } catch (Exception e) {
            throw new AdbClientException(AdbClientException.ADD_DATA_ERROR, String.format("Add row data (%s) error: %s", row.getColumnValues().toString(), e.getMessage()), e);
        } finally {
            closeDBResources(null, null, conn);
        }
    }

    /**
     * Add Map data
     *
     * @param table  String
     * @param oriMap Map
     */
    public void addMap(String table, Map<String, String> oriMap) {
        String tableName = table.toLowerCase();
        Map<String, String> dataMap = new HashMap<String, String>();
        for (Map.Entry<String, String> entry : oriMap.entrySet()) {
            dataMap.put(entry.getKey().toLowerCase(), entry.getValue());
        }
        if (this.tableInfo.get(tableName) == null) {
            throw new AdbClientException(AdbClientException.CONFIG_ERROR, "The table " + tableName + " do not exist", null);
        }
        Row row = mapToRow(tableName, dataMap);
        addRow(tableName, row);
    }


    public void addRows(String tableName, List<Row> rows) {
        for (Row row : rows) {
            addRow(tableName, row);
        }
    }

    public void addMaps(String tableName, List<Map<String, String>> maps) {
        for (Map<String, String> map : maps) {
            addMap(tableName, map);
        }
    }

    public void addStrictMap(String table, Map<String, String> oriMap) {
        String tableName = table.toLowerCase();
        Map<String, String> dataMap = new HashMap<String, String>();
        for (Map.Entry<String, String> entry : oriMap.entrySet()) {
            dataMap.put(entry.getKey().toLowerCase(), entry.getValue());
        }
        if (this.tableInfo.get(tableName) == null) {
            initNewTable(tableName);
            if (this.tableInfo.get(tableName) == null) {
                throw new AdbClientException(AdbClientException.ADD_DATA_ERROR, "The table " + table + " do not exist", null);
            }
        }
        Row row = mapToRow(tableName, dataMap);
        if (dataMap.size() > 0) {
            if (!this.isAllColumn.get(tableName)) {
                throw new AdbClientException(AdbClientException.ADD_DATA_ERROR, "The column " + dataMap.keySet().toString() + " of table " + tableName + " do not exist", null);
            }
            dataMap.clear();
            initNewTable(tableName);
            for (Map.Entry<String, String> entry : oriMap.entrySet()) {
                dataMap.put(entry.getKey().toLowerCase(), entry.getValue());
            }
            row = mapToRow(tableName, dataMap);
            if (dataMap.size() > 0) {
                throw new AdbClientException(AdbClientException.ADD_DATA_ERROR, "The columns " + dataMap.keySet().toString() + " of table " + tableName + " do not exist", null);
            }
            commitSingleTable(tableName);
        }
        addRow(tableName, row);
    }

    public void addStrictMaps(String tableName, List<Map<String, String>> maps) {
        for (Map<String, String> map : maps) {
            addStrictMap(tableName, map);
        }
    }

    /**
     * 调用方主动触发提交，会执行缓存中的所有sql
     */
    public void commit() {
        try {
            if (commitExceptionDataList.size() > 0 || commitException != null) {
                commitExceptionDataList.clear();
                commitException = null;
            }
            if (databaseConfig.isPartitionBatch()) {
                for (Map<Integer, MutablePair<StringBuilder, Integer>> partitionString : partitionBatch.values()) {
                    for (Map.Entry<Integer, MutablePair<StringBuilder, Integer>> entry : partitionString.entrySet()) {
                        sqlQueue.put(entry.getValue().getLeft());
                    }
                }
            } else {
                for (Map.Entry<String, StringBuilder> entry : batchBuffer.entrySet()) {
                    sqlQueue.put(entry.getValue());
                }
            }
        } catch (Exception e) {
            logger("error", e.getMessage());
            throw new AdbClientException(AdbClientException.COMMIT_ERROR_OTHER, e.getMessage(), e);
        }
        sqlQueueExecute();
    }

    private void commitSingleTable(String table) {
        if (partitionBatch.get(table) == null || partitionBatch.get(table).size() == 0) {
            return;
        }
        try {
            if (commitExceptionDataList.size() > 0 || commitException != null) {
                commitExceptionDataList.clear();
                commitException = null;
            }
            if (databaseConfig.isPartitionBatch()) {
                for (Map.Entry<Integer, MutablePair<StringBuilder, Integer>> entry : partitionBatch.get(table).entrySet()) {
                    sqlQueue.put(entry.getValue().getLeft());
                }
            } else {
                for (Map.Entry<String, StringBuilder> entry : batchBuffer.entrySet()) {
                    sqlQueue.put(entry.getValue());
                }
            }
        } catch (Exception e) {
            logger("error", e.getMessage());
            throw new AdbClientException(AdbClientException.COMMIT_ERROR_OTHER, e.getMessage(), e);
        }
        sqlQueueExecute();
    }

    private void sqlQueueExecute() {
        if (isSync) {
            while (true) {
                StringBuilder sb = sqlQueue.poll();
                if (sb == null) {
                    break;
                }
                executeBatchSql(sb);
            }
            if (databaseConfig.isPartitionBatch()) {
                partitionBatch.clear();
            } else {
                batchBuffer.clear();
            }
            if (System.currentTimeMillis() - periodStartTime > periodTime) {
                periodStartTime = System.currentTimeMillis();
                logger("info", "Total record count " + totalCount);
            }
        } else {
            List<Future> futureList = new ArrayList<Future>();
            final CountDownLatch latch = new CountDownLatch(databaseConfig.getParallelNumber());
            for (int i = 0; i < databaseConfig.getParallelNumber(); i++) {
                Future future = executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            while (true) {
                                StringBuilder sb = sqlQueue.poll();
                                if (sb == null) {
                                    break;
                                }
                                executeBatchSql(sb);
                            }
                        } finally {
                            latch.countDown();
                        }
                    }
                });
                futureList.add(future);
            }

            try {
                latch.await();
                for (Future f : futureList) {
                    f.get();
                }
                if (databaseConfig.isPartitionBatch()) {
                    partitionBatch.clear();
                } else {
                    batchBuffer.clear();
                }
                if (System.currentTimeMillis() - periodStartTime > periodTime) {
                    periodStartTime = System.currentTimeMillis();
                    logger("info", "Total record count " + totalCount);
                }
            } catch (AdbClientException e) {
                logger("error", "commit " + e.getMessage());
                throw e;
            } catch (Exception e) {
                logger("error", "commit " + e.getMessage());
                throw new RuntimeException(e.getMessage());
            }
            if (commitExceptionDataList.size() > 0) {
                logger("error", "commit error data list " + commitExceptionDataList.toString());
                throw new AdbClientException(AdbClientException.COMMIT_ERROR_DATA_LIST, commitExceptionDataList, commitException);
            }
        }

    }


    public Connection getConnection() {
        int retryNum = 0;
        Exception ex = null;
        while (retryNum <= databaseConfig.getRetryTimes()) {
            try {
                // Max wait time 60s
                return dataSource.getConnection(60000);
            } catch (Exception e) {
                ex = e;
                logger("error", "Create connection error after " + retryNum + " times retry " + e.getMessage());
            }
            retryNum++;
            try {
                TimeUnit.MILLISECONDS.sleep(databaseConfig.getRetryIntervalTime());
            } catch (InterruptedException e) {
                logger("error", "create connection error " + e.getMessage());
            }
        }
        throw new AdbClientException(AdbClientException.CREATE_CONNECTION_ERROR, "Creating statement and connection failed", ex);
    }

    public void stop() {
        if (databaseConfig.isPartitionBatch()) {
            if (partitionBatch.size() > 0 || sqlQueue.size() > 0) {
                throw new AdbClientException(AdbClientException.STOP_ERROR, "Batch data do not commit, please commit first", null);
            }
        } else {
            if (batchBuffer.size() > 0) {
                throw new AdbClientException(AdbClientException.STOP_ERROR, "Batch data do not commit, please commit first", null);
            }
        }
        forceStop();
    }

    public void forceStop() {
        this.partitionBatch.clear();
        this.sqlQueue.clear();
        if (executorService != null) {
            this.executorService.shutdown();
        }
        if (this.batchBuffer != null) {
            this.batchBuffer.clear();
        }
        if (this.partitionBatch != null) {
            this.partitionBatch.clear();
        }
        this.tableInfo.clear();
        this.tableColumnsMetaData.clear();
        this.configColumnsMetaData.clear();
    }

    public TableInfo getTableInfo(String tableName) {
        return this.tableInfo.get(tableName);
    }

    public List<ColumnInfo> getColumnInfo(String tableName) {
        return this.tableInfo.get(tableName).getColumns();
    }

    /**
     * Init druid datasource
     */
    private void initDatasource() {
        if (dataSource != null) {
            Connection testConn = null;
            try {
                testConn = this.getConnection();
            } finally {
                closeDBResources(null, null, testConn);
            }
            return;
        }
        if (databaseConfig.isShareDataSource()) {
            this.dataSource = clientDataSource.getDataSource(databaseConfig);
            return;
        }
        this.dataSource = clientDataSource.newDataSource(databaseConfig);
    }

    /**
     * Check userConfig and ads tableConfig
     *
     * @return Boolean
     */
    private Boolean checkConfig() {
        try {
            checkDatabaseConfig();
            getTableInfo(databaseConfig.getDatabase(), databaseConfig.getTable(), this.getConnection());
            for (String tableName : databaseConfig.getTable()) {
                checkTableConfig(tableName);
            }
            return true;
        } catch (Exception e) {
            throw new AdbClientException(AdbClientException.CONFIG_ERROR, "Check config exception: " + e.getMessage(), e);
        }
    }

    /**
     * Deal with user's column config
     *
     * @param tableName    String
     * @param tableColumns List<String>
     */
    private void dealColumnConf(String tableName, List<String> tableColumns) {
        List<String> userConfiguredColumns = databaseConfig.getColumns(tableName);
        if (null == userConfiguredColumns || userConfiguredColumns.isEmpty()) {
            throw new AdbClientException(AdbClientException.CONFIG_ERROR, "Config is error. Do not have column list", null);
        } else {
            if (1 == userConfiguredColumns.size() && ALL_COLUMN_CHARACTER.equals(userConfiguredColumns.get(0))) {
                this.isAllColumn.put(tableName, true);
                databaseConfig.setColumns(tableName, tableColumns);
            } else if (userConfiguredColumns.size() > tableColumns.size()) {
                throw new AdbClientException(AdbClientException.CONFIG_ERROR, String.format("Database config is error. The count of writer columns %s is bigger than the count of read table's columns {}.",
                        userConfiguredColumns.size(), tableColumns.size()), null);
            } else {
                makeSureNoValueDuplicate(userConfiguredColumns, false);
                List<String> removeQuotedColumns = new ArrayList<String>();
                for (String each : userConfiguredColumns) {
                    if (each.startsWith(COLUMN_QUOTE_CHARACTER) && each.endsWith(COLUMN_QUOTE_CHARACTER)) {
                        removeQuotedColumns.add(each.substring(1, each.length() - 1));
                    } else {
                        removeQuotedColumns.add(each);
                    }
                }
                makeSureBInA(tableColumns, removeQuotedColumns, false);
            }
        }
    }

    /**
     * Generate insert sql
     *
     * @param tableName  String
     * @param connection Connection
     * @param record     Row
     * @return String
     * @throws SQLException SQLException
     */
    private String generateInsertSql(String tableName, Connection connection, Row record) throws SQLException {
        String sql = null;
        StringBuilder sqlSb = new StringBuilder();
        sqlSb.append(this.insertSqlPrefix.get(tableName));
        sqlSb.append("(");
        int columnsSize = this.databaseConfig.getColumns(tableName).size();
        for (int i = 0; i < columnsSize; i++) {
            if ((i + 1) != columnsSize) {
                sqlSb.append("?,");
            } else {
                sqlSb.append("?");
            }
        }
        sqlSb.append(")");
        //mysql impl warn: if a database access error occurs or this method is called on a closed connection
        PreparedStatement statement = connection.prepareStatement(sqlSb.toString());
        for (int i = 0; i < this.databaseConfig.getColumns(tableName).size(); i++) {
            String columnName = this.databaseConfig.getColumns(tableName).get(i);
            int columnSqltype = this.configColumnsMetaData.get(tableName).get(columnName).getLeft();
            if (record.getColumnValues().get(i) == null) {
                if (this.tableInfo.get(tableName).getColumns().get(i).getDefaultValue() != null) {
                    prepareColumnTypeValue(statement, columnSqltype, this.tableInfo.get(tableName).getColumns().get(i).getDefaultValue(), i, columnName, tableName);
                } else {
                    prepareColumnTypeValue(statement, columnSqltype, null, i, columnName, tableName);
                }
            } else {
                prepareColumnTypeValue(statement, columnSqltype, record.getColumnValues().get(i).toString(), i, columnName, tableName);
            }
        }
//        sql = ((JDBC4PreparedStatement) ((DruidPooledPreparedStatement) statement).getRawPreparedStatement()).asSql();
        sql = statement.unwrap(com.mysql.jdbc.PreparedStatement.class).asSql();
        closeDBResources(null, statement, null);
        return sql;
    }


    /**
     * Get ads table and column info
     *
     * @param schema String
     * @param tables List<String>
     */
    private void getTableInfo(String schema, List<String> tables, Connection connection) {
        if (tables == null) {
            throw new RuntimeException("tables is not exist");
        }
        StringBuilder tablesSb = new StringBuilder();
        int i = 1;
        for (String table : tables) {
            tablesSb.append("'").append(table).append("'");
            if (i < tables.size()) {
                tablesSb.append(",");
            }
            i++;
        }
        Statement statement = null;
        ResultSet rs = null;
        try {
            statement = connection.createStatement();
            String columnMetaSql = String.format("select ordinal_position,column_name,data_type,type_name,column_comment, is_nullable, column_default, table_name from information_schema.columns where table_schema = '%s' and table_name in ( %s ) order by ordinal_position",
                    schema.toLowerCase(),
                    tablesSb.toString());
            rs = statement.executeQuery(columnMetaSql);

            Map<String, List<ColumnInfo>> columnInfoListMap = new HashMap<String, List<ColumnInfo>>();
            while (rs.next()) {
                if (columnInfoListMap.get(rs.getString(8)) == null) {
                    columnInfoListMap.put(rs.getString(8), new ArrayList<ColumnInfo>());
                }
                ColumnInfo columnInfo = new ColumnInfo();
                columnInfo.setOrdinal(rs.getInt(1));
                columnInfo.setName(rs.getString(2).toLowerCase());
                //for ads version 0.8 & 0.7
                columnInfo.setDataType(ColumnDataType.getTypeByName(rs.getString(4).toUpperCase()));
                columnInfo.setComment(rs.getString(5));
                columnInfo.setNullable(("YES").equals(rs.getString(6)));
                columnInfo.setDefaultValue(rs.getString(7));
                columnInfoListMap.get(rs.getString(8)).add(columnInfo);
            }
            for (String table : tables) {
                if (columnInfoListMap.get(table) == null || columnInfoListMap.get(table).isEmpty()) {
                    logger("error", "Table" + table + " is not existed or do not has any column");
                }
            }
            closeDBResources(rs, statement, null);

            String tableMetaSql = String.format("select update_type, partition_type, partition_column, partition_count, primary_key_columns, sub_partition_column, table_name from information_schema.tables where table_schema = '%s' and table_name in ( %s )",
                    schema.toLowerCase(),
                    tablesSb.toString());
            statement = connection.createStatement();
            rs = statement.executeQuery(tableMetaSql);
            while (rs.next()) {
                String t = rs.getString(7);
                TableInfo tableInfoTmp = new TableInfo();
                tableInfoTmp.setColumns(columnInfoListMap.get(t));
                tableInfoTmp.setTableSchema(schema);
                tableInfoTmp.setTableName(t);

                tableInfoTmp.setUpdateType(rs.getString(1));
                tableInfoTmp.setPartitionType(rs.getString(2));
                tableInfoTmp.setPartitionColumn(StringUtils.isNotBlank(rs.getString(3)) ? rs.getString(3).toLowerCase() : null);
                tableInfoTmp.setPartitionCount(rs.getInt(4));
                //primary_key_columns  ads pk split by ','
                String primaryKeyColumns = rs.getString(5);
                if (StringUtils.isNotBlank(primaryKeyColumns)) {
                    tableInfoTmp.setPrimaryKeyColumns(Arrays.asList(StringUtils.split(primaryKeyColumns.toLowerCase(), ",")));
                } else {
                    tableInfoTmp.setPrimaryKeyColumns(null);
                }
                tableInfoTmp.setSubPartitionColumn(StringUtils.isNotBlank(rs.getString(6)) ? rs.getString(6).toLowerCase() : null);
                this.tableInfo.put(t, tableInfoTmp);
            }
            closeDBResources(rs, statement, null);
            for (String table : tables) {
                if (this.tableInfo.get(table) == null) {
                    logger("error", "Table" + table + " is not existed or do not has any column");
//                    throw new AdbClientException(AdbClientException.CONFIG_ERROR, "Table" + table + " is not existed or do not has any column", null);
                }
            }
        } catch (Exception e) {
            throw new AdbClientException(AdbClientException.CONFIG_ERROR, "GetTableInfo exception: " + e.getMessage(), e);
        } finally {
            closeDBResources(rs, statement, connection);
        }
    }


    /**
     * Close db resources
     *
     * @param rs   ResultSet
     * @param stmt Statement
     * @param conn Connection
     */
    private void closeDBResources(ResultSet rs, Statement stmt, Connection conn) {
        List<String> errList = new ArrayList<String>();
        Exception ex = null;
        if (null != rs) {
            try {
                rs.close();
            } catch (Exception e) {
                errList.add("Close ResultSet occur SQLException " + e.getMessage());
                ex = e;
            }
        }

        if (null != stmt) {
            try {
                stmt.close();
            } catch (Exception e) {
                errList.add("Close Statement occur SQLException " + e.getMessage());
                ex = e;
            }
        }

        if (null != conn) {
            try {
                conn.close();
            } catch (Exception e) {
                errList.add("Close Connection occur SQLException " + e.getMessage());
                ex = e;
            }
        }
        if (errList.size() > 0) {
            logger("error", AdbClientException.CLOSE_CONNECTION_ERROR + errList.toString());
//            throw new AdbClientException(AdbClientException.CLOSE_CONNECTION_ERROR, errList.toString(), ex);
        }

    }

    private Map<String, Pair<Integer, String>> getColumnMetaData(TableInfo tableInfo, List<String> userColumns) {
        Map<String, Pair<Integer, String>> columnMetaData = new HashMap<String, Pair<Integer, String>>();
        List<ColumnInfo> columnInfoList = tableInfo.getColumns();
        for (String column : userColumns) {
            if (column.startsWith(COLUMN_QUOTE_CHARACTER) && column.endsWith(COLUMN_QUOTE_CHARACTER)) {
                column = column.substring(1, column.length() - 1);
            }
            for (ColumnInfo columnInfo : columnInfoList) {
                if (column.equalsIgnoreCase(columnInfo.getName())) {
                    Pair<Integer, String> eachPair = new ImmutablePair<Integer, String>(columnInfo.getDataType().sqlType, columnInfo.getDataType().name);
                    columnMetaData.put(columnInfo.getName(), eachPair);
                }
            }
        }
        return columnMetaData;
    }

    private void prepareColumnTypeValue(PreparedStatement statement, int columnSqltype, String column, int preparedPatamIndex, String columnName, String tableName) throws SQLException {
        java.util.Date utilDate;
        switch (columnSqltype) {
            case Types.CHAR:
            case Types.NCHAR:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                statement.setString(preparedPatamIndex + 1, column);
                break;

            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.REAL:
                if (databaseConfig.getEmptyAsNull() && "".equals(column) || column == null) {
                    statement.setNull(preparedPatamIndex + 1, Types.BIGINT);
                } else {
                    statement.setLong(preparedPatamIndex + 1, Long.parseLong(column.trim()));
                }
                break;

            case Types.DECIMAL:
            case Types.NUMERIC:
                if (databaseConfig.getEmptyAsNull() && "".equals(column) || column == null) {
                    statement.setNull(preparedPatamIndex + 1, Types.DECIMAL);
                } else {
                    statement.setBigDecimal(preparedPatamIndex + 1, new BigDecimal(column.trim()));
                }
                break;

            case Types.FLOAT:
            case Types.DOUBLE:
                if (databaseConfig.getEmptyAsNull() && "".equals(column) || column == null) {
                    statement.setNull(preparedPatamIndex + 1, Types.DOUBLE);
                } else {
                    statement.setDouble(preparedPatamIndex + 1, Double.parseDouble(column.trim()));
                }
                break;

            //tinyint is a little special in some database like mysql {boolean->tinyint(1)}
            case Types.TINYINT:
                if (databaseConfig.getEmptyAsNull() && "".equals(column) || null == column) {
                    statement.setNull(preparedPatamIndex + 1, Types.BIGINT);
                } else {
                    statement.setLong(preparedPatamIndex + 1, Long.valueOf(column.trim()));
                }
                break;

            case Types.DATE:
                java.sql.Date sqlDate = null;
                try {
                    if (column == null || "".equals(column)) {
                        utilDate = null;
                    } else {
                        utilDate = new SimpleDateFormat("yyyy-MM-dd").parse(column.trim());
                    }
                } catch (Exception e) {
                    if (e instanceof ParseException) {
                        try {
                            utilDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(column.trim());
                        } catch (Exception ex) {
                            throw new SQLException(String.format(
                                    "Date transform error：[%s]", column), ex);
                        }
                    } else {
                        throw new SQLException(String.format(
                                "Date transform error：[%s]", column), e);
                    }
                }

                if (null != utilDate) {
                    sqlDate = new java.sql.Date(utilDate.getTime());
                }
                statement.setDate(preparedPatamIndex + 1, sqlDate);
                break;

            case Types.TIME:
                Time sqlTime = null;
                try {
                    if (column == null || "".equals(column)) {
                        utilDate = null;
                    } else {
                        utilDate = new SimpleDateFormat("HH:mm:ss").parse(column.trim());
                    }
                } catch (Exception e) {
                    if (e instanceof ParseException) {
                        try {
                            utilDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(column.trim());
                        } catch (Exception ex) {
                            throw new SQLException(String.format(
                                    "TIME transform error：[%s]", column), ex);
                        }
                    } else {
                        throw new SQLException(String.format(
                                "TIME transform error：[%s]", column));
                    }
                }

                if (null != utilDate) {
                    sqlTime = new Time(utilDate.getTime());
                }
                statement.setTime(preparedPatamIndex + 1, sqlTime);
                break;

            case Types.TIMESTAMP:
                Timestamp sqlTimestamp = null;
                try {
                    if (column == null || "".equals(column)) {
                        utilDate = null;
                    } else {
                        utilDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(column.trim());
                    }
                } catch (Exception e) {
                    throw new SQLException(String.format(
                            "TIMESTAMP transform error：[%s]", column));
                }

                if (null != utilDate) {
                    sqlTimestamp = new Timestamp(utilDate.getTime());
                }
                statement.setTimestamp(preparedPatamIndex + 1, sqlTimestamp);
                break;

            case Types.BOOLEAN:
                //case Types.BIT: ads 没有bit
                if (null == column || "".equals(column) && databaseConfig.getEmptyAsNull()) {
                    statement.setNull(preparedPatamIndex + 1, Types.BOOLEAN);
                } else {
                    statement.setBoolean(preparedPatamIndex + 1, Boolean.parseBoolean(column.trim()));
                }

                break;
            default:
//                statement.setString(preparedPatamIndex + 1, column);
                Pair<Integer, String> columnMetaPair = this.configColumnsMetaData.get(tableName).get(columnName);
                throw new AdbClientException(AdbClientException.CONFIG_ERROR, String.format("Your config is illegal. Because ADB does not support this type: column:[%s], type:[%s], Java type:[%s].",
                        columnName, columnMetaPair.getRight(), columnMetaPair.getLeft()), null);
        }
    }

    private static void makeSureNoValueDuplicate(List<String> aList, boolean caseSensitive) {
        if (null == aList || aList.isEmpty()) {
            throw new RuntimeException("Column can not be null");
        }

        if (1 == aList.size()) {
            return;
        } else {
            List<String> list = null;
            if (!caseSensitive) {
                list = valueToLowerCase(aList);
            } else {
                list = new ArrayList<String>(aList);
            }

            Collections.sort(list);

            for (int i = 0, len = list.size() - 1; i < len; i++) {
                if (list.get(i).equals(list.get(i + 1))) {
                    throw new RuntimeException(String.format("The column %s in config must be uniq", list.get(i)));
                }
            }
        }
    }


    private static List<String> valueToLowerCase(List<String> aList) {
        if (null == aList || aList.isEmpty()) {
            throw new RuntimeException("Column can not be null");
        }
        List<String> result = new ArrayList<String>(aList.size());
        for (String oneValue : aList) {
            result.add(null != oneValue ? oneValue.toLowerCase() : null);
        }
        return result;
    }

    private static void makeSureBInA(List<String> aList, List<String> bList, boolean caseSensitive) {
        if (null == aList || aList.isEmpty() || null == bList || bList.isEmpty()) {
            throw new RuntimeException("Column can not be null");
        }

        List<String> all = null;
        List<String> part = null;

        if (!caseSensitive) {
            all = valueToLowerCase(aList);
            part = valueToLowerCase(bList);
        } else {
            all = new ArrayList<String>(aList);
            part = new ArrayList<String>(bList);
        }

        for (String oneValue : part) {
            if (!all.contains(oneValue)) {
                throw new RuntimeException(String.format("The column %s is not exist in table", oneValue));
            }
        }
    }

    /**
     * Get ads hash partition id
     *
     * @param tableName String
     * @param row       Row
     * @return int
     */
    private int getHashPartition(String tableName, Row row) {
        String value = null;
        if (partitionColumnIndex.get(tableName) != null && partitionColumnIndex.get(tableName) != -1 && row.getColumnValues().get(partitionColumnIndex.get(tableName)) != null) {
            value = row.getColumnValues().get(partitionColumnIndex.get(tableName)).toString();
        }
        if (value == null) {
            for (String pk : tableInfo.get(tableName).getPrimaryKeyColumns()) {
                if (tableInfo.get(tableName).getSubPartitionColumn() == null || !pk.equalsIgnoreCase(tableInfo.get(tableName).getSubPartitionColumn())) {
                    int subPartitionIndex = databaseConfig.getColumns(tableName).indexOf(pk);
                    if (row.getColumnValues().get(subPartitionIndex) != null) {
                        value = row.getColumnValues().get(subPartitionIndex).toString();
                        break;
                    }
                }
            }
            if (value == null) {
                return new Random().nextInt(tableInfo.get(tableName).getPartitionCount());
            }
        }
        long crc32 = getCRC32(value);
        return (int) (crc32 % tableInfo.get(tableName).getPartitionCount());
    }

    private static long getCRC32(String value) {
        Checksum checksum = new CRC32();
        byte[] bytes = value.getBytes();
        checksum.update(bytes, 0, bytes.length);
        return checksum.getValue();
    }

    /**
     * Check the databaseConfig
     */
    private void checkDatabaseConfig() {
        if (databaseConfig.getTable() == null || databaseConfig.getTable().size() == 0) {
            throw new RuntimeException("Table can not be null");
        }
        for (String tableName : databaseConfig.getTable()) {
            if (databaseConfig.getColumns(tableName) == null) {
                throw new RuntimeException(String.format("Columns of table %s can not be null", tableName));
            }
        }
        if (databaseConfig.getHost() == null) {
            throw new RuntimeException("Host can not be null");
        }
        if (databaseConfig.getDatabase() == null) {
            throw new RuntimeException("Database can not be null");
        }
        if (databaseConfig.getPassword() == null) {
            throw new RuntimeException("Password can not be null");
        }
        if (databaseConfig.getUser() == null) {
            throw new RuntimeException("Username can not be null");
        }
        if (databaseConfig.getPort() == 0) {
            throw new RuntimeException("Port can not be 0");
        }
        if (databaseConfig.getEmptyAsNull() == null) {
            throw new RuntimeException("EmptyAsNull can not be null");
        }
    }

    /**
     * Execute batch sql
     *
     * @param sb StringBuilder
     */
    private void executeBatchSql(StringBuilder sb) {
        if (sb.length() != 0) {
            int retryNum = 0;
            Connection conn = null;
            Statement stmt = null;
            while (retryNum <= databaseConfig.getRetryTimes()) {
                try {
                    conn = this.getConnection();
                    stmt = conn.createStatement();
                    stmt.execute(sb.toString());
                    break;
//                } catch (MySQLSyntaxErrorException e) {
//                    if (databaseConfig.isInsertExceptionSplit()) {
//                        commitException = e;
//                        executeEachRow(sb);
//                        break;
//                    }
                } catch (Exception e) {
                    if (retryNum == databaseConfig.getRetryTimes()) {
                        logger("error", sb.toString());
                        throw new AdbClientException(AdbClientException.COMMIT_ERROR_OTHER, String.format("Commit failed after %s times retry, please commit again later! Detail of exception is %s", retryNum, e.getMessage()), e);
                    }
                } finally {
                    closeDBResources(null, stmt, conn);
                }
                retryNum++;
                try {
                    TimeUnit.MILLISECONDS.sleep(databaseConfig.getRetryIntervalTime());
                } catch (InterruptedException e) {
                    logger("error", "commit error " + e.getMessage());
                }
            }
        }
    }

    /**
     * Execute each row which is splited
     *
     * @param sb StringBuilder
     */
    private void executeEachRow(StringBuilder sb) {
        Statement stmt = null;
        Connection conn = null;
        try {
            conn = this.getConnection();
            stmt = conn.createStatement();
        } catch (SQLException ec) {
            closeDBResources(null, stmt, conn);
            throw new AdbClientException(AdbClientException.CREATE_CONNECTION_ERROR, "Creating statement and connection failed when commit: " + ec.getMessage(), ec);
        }
        Map<String, String[]> splitSqlMap = splitBatchSql(sb);
        for (Map.Entry<String, String[]> entry : splitSqlMap.entrySet()) {
            for (String splitSql : entry.getValue()) {
                try {
                    stmt.execute(insertSqlPrefix.get(entry.getKey()) + splitSql);
                } catch (Exception ex) {
                    closeDBResources(null, stmt, conn);
                    try {
                        conn = this.getConnection();
                        stmt = conn.createStatement();
                    } catch (SQLException ec) {
                        closeDBResources(null, stmt, conn);
                        throw new AdbClientException(AdbClientException.CREATE_CONNECTION_ERROR, "Creating statement and connection failed when commit: " + ec.getMessage(), ec);
                    }
                    if (!databaseConfig.isIgnoreInsertError()) {
                        commitExceptionDataList.add(splitSql);
                    } else {
                        exceptionCount.incrementAndGet();
                    }
                }
            }
        }
        closeDBResources(null, stmt, conn);
    }

    /**
     * Split sql values into string[]
     *
     * @param sb StringBuilder
     * @return Map<String, String[]>
     */
    private Map<String, String[]> splitBatchSql(StringBuilder sb) {
        String tableName = null;
        if (databaseConfig.isInsertIgnore()) {
            tableName = sb.toString().split(" ")[3];
        } else {
            tableName = sb.toString().split(" ")[2];
        }
        StringBuilder dataBuffer = sb.replace(0, insertSqlPrefix.get(tableName).length(), "");
        Map<String, String[]> resMap = new HashMap<String, String[]>();
        resMap.put(tableName, dataBuffer.toString().split(SQL_SPLIT_CHARACTER));
        return resMap;
    }

    private void logger(String level, String msg) {
        if (databaseConfig.getLogger() != null) {
            if ("info".equals(level)) {
                databaseConfig.getLogger().info("Adb Client info: {}", msg);
            } else if ("error".equals(level)) {
                databaseConfig.getLogger().error("Adb Client error: {}", msg);
            }
        }
    }

    private void checkTableConfig(String tableName) {
        TableInfo tableInfoTmp = this.tableInfo.get(tableName);
        if (tableInfoTmp == null) {
            return;
        }
        if (!"realtime".equals(tableInfoTmp.getUpdateType().toLowerCase())) {
            throw new RuntimeException("table " + tableName + " is not realtime table, can not insert by Adb Client");
        }
        List<String> allColumns = new ArrayList<String>();
        List<ColumnInfo> columnInfo = tableInfoTmp.getColumns();
        for (ColumnInfo eachColumn : columnInfo) {
            allColumns.add(eachColumn.getName());
        }
        dealColumnConf(tableName, allColumns);

        this.tableColumnsMetaData.put(tableName, getColumnMetaData(tableInfoTmp, this.databaseConfig.getColumns(tableName)));
        Map<String, Pair<Integer, String>> configColumnMetaDataTmp = new HashMap<String, Pair<Integer, String>>();

        for (int i = 0; i < this.databaseConfig.getColumns(tableName).size(); i++) {
            String oriEachColumn = this.databaseConfig.getColumns(tableName).get(i);
            String eachColumn = oriEachColumn;
            // 防御性保留字
            if (eachColumn.startsWith(COLUMN_QUOTE_CHARACTER) && eachColumn.endsWith(COLUMN_QUOTE_CHARACTER)) {
                eachColumn = eachColumn.substring(1, eachColumn.length() - 1);
            }
            for (String eachAdsColumn : tableInfoTmp.getColumnsNames()) {
                if (eachColumn.equalsIgnoreCase(eachAdsColumn)) {
                    configColumnMetaDataTmp.put(oriEachColumn, this.tableColumnsMetaData.get(tableName).get(eachAdsColumn));
                }
            }
            if (eachColumn.equalsIgnoreCase(tableInfoTmp.getPartitionColumn())) {
                this.partitionColumnIndex.put(tableName, i);
            }
        }
        this.configColumnsMetaData.put(tableName, configColumnMetaDataTmp);

        // insertSqlPrefix init
        if (isAllColumn.get(tableName)) {
            if (!databaseConfig.isInsertWithColumnName()) {
                if (databaseConfig.isInsertIgnore()) {
                    this.insertSqlPrefix.put(tableName, String.format(INSERT_IGNORE_ALL_COLUMN_TEMPLATE, tableName));
                } else {
                    this.insertSqlPrefix.put(tableName, String.format(INSERT_ALL_COLUMN_TEMPLATE, tableName));
                }
            } else {
                if (databaseConfig.isInsertIgnore()) {
                    this.insertSqlPrefix.put(tableName, String.format(INSERT_IGNORE_TEMPLATE, tableName, StringUtils.join(this.databaseConfig.getColumns(tableName), ",")));
                } else {
                    this.insertSqlPrefix.put(tableName, String.format(INSERT_TEMPLATE, tableName, StringUtils.join(this.databaseConfig.getColumns(tableName), ",")));
                }
            }
        } else {
            if (databaseConfig.isInsertIgnore()) {
                this.insertSqlPrefix.put(tableName, String.format(INSERT_IGNORE_TEMPLATE, tableName, StringUtils.join(this.databaseConfig.getColumns(tableName), ",")));
            } else {
                this.insertSqlPrefix.put(tableName, String.format(INSERT_TEMPLATE, tableName, StringUtils.join(this.databaseConfig.getColumns(tableName), ",")));
            }
        }
    }


    private Row mapToRow(String tableName, Map<String, String> dataMap) {
        Row row = new Row();
        int i = 0;
        if (this.isAllColumn.get(tableName)) {
            for (ColumnInfo ci : this.tableInfo.get(tableName).getColumns()) {
                if (dataMap.get(ci.getName()) != null) {
                    row.setColumn(i, dataMap.get(ci.getName()));
                    dataMap.remove(ci.getName());
                } else {
                    if (!ci.isNullable()) {
                        throw new AdbClientException(AdbClientException.CONFIG_ERROR, String.format("The column %s of table %s can not be null", ci.getName(), tableName), null);
                    }
                    row.setColumn(i, ci.getDefaultValue());
                }
                i++;
            }
        } else {
            for (String col : this.databaseConfig.getColumns(tableName)) {
                if (dataMap.get(col) != null) {
                    row.setColumn(i, dataMap.get(col));
                    dataMap.remove(col);
                } else {
                    for (ColumnInfo ci : this.tableInfo.get(tableName).getColumns()) {
                        if (ci.getName().equalsIgnoreCase(col)) {
                            if (!ci.isNullable()) {
                                throw new AdbClientException(AdbClientException.CONFIG_ERROR, String.format("The column %s of table %s can not be null", ci.getName(), tableName), null);
                            }
                            row.setColumn(i, ci.getDefaultValue());
                        }
                    }
                }
                i++;
            }
        }
        return row;
    }
}