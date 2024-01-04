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

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * AnalyticDB client database config
 *
 * @author caihua
 */
public class DatabaseConfig {
    private String host;
    private int port;
    private String database;
    private String user;
    private String password;
    private List<String> table;
    private boolean ignoreInsertError = false;
    private boolean insertIgnore = false;
    private Map<String, List<String>> tableColumns = new HashMap<String, List<String>>();
    private Boolean emptyAsNull = true;
    private Logger logger;
    private int parallelNumber = 4;
    private int retryTimes = 3;
    private boolean shareDataSource = true;
    /**
     * Retry interval time (ms)
     */
    private long retryIntervalTime = 1000;
    private boolean partitionBatch = true;
    private long commitSize = AdbClient.DEFAULT_SQL_LENGTH_LIMIT;
    private boolean insertWithColumnName = true;
    private boolean insertExceptionSplit = true;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database.toLowerCase();
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public List<String> getTable() {
        return table;
    }

    public void setTable(List<String> table) {
        this.table = new ArrayList<String>();
        for (String t : table) {
            this.table.add(t.toLowerCase());
        }
    }

    public boolean isIgnoreInsertError() {
        return ignoreInsertError;
    }

    public void setIgnoreInsertError(boolean ignoreInsertError) {
        this.ignoreInsertError = ignoreInsertError;
    }

    public List<String> getColumns(String tableName) {
        return tableColumns.get(tableName);
    }

    public void setColumns(String tableName, List<String> columns) {
        List<String> col = new ArrayList<String>();
        for (String c : columns) {
            col.add(c.toLowerCase());
        }
        this.tableColumns.put(tableName.toLowerCase(), col);
    }

    public void setEmptyAsNull(Boolean emptyAsNull) {
        this.emptyAsNull = emptyAsNull;
    }

    public Boolean getEmptyAsNull() {
        return this.emptyAsNull;
    }

    public boolean isInsertIgnore() {
        return insertIgnore;
    }

    public void setInsertIgnore(boolean insertIgnore) {
        this.insertIgnore = insertIgnore;
    }

    public Logger getLogger() {
        return logger;
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    public int getParallelNumber() {
        return parallelNumber;
    }

    public void setParallelNumber(int parallelNumber) {
        this.parallelNumber = parallelNumber;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public long getRetryIntervalTime() {
        return retryIntervalTime;
    }

    public void setRetryIntervalTime(long retryIntervalTime) {
        this.retryIntervalTime = retryIntervalTime;
    }

    public boolean isPartitionBatch() {
        return partitionBatch;
    }

    public void setPartitionBatch(boolean partitionBatch) {
        this.partitionBatch = partitionBatch;
    }

    public boolean isShareDataSource() {
        return shareDataSource;
    }

    public void setShareDataSource(boolean shareDataSource) {
        this.shareDataSource = shareDataSource;
    }

    public void setCommitSize(long commitSize) {
        this.commitSize = commitSize;
    }

    public long getCommitSize() {
        return this.commitSize;
    }

    public void setInsertWithColumnName(boolean insertWithColumnName) {
        this.insertWithColumnName = insertWithColumnName;
    }

    public boolean isInsertWithColumnName() {
        return insertWithColumnName;
    }

    public boolean isInsertExceptionSplit() {
        return insertExceptionSplit;
    }

    public void setInsertExceptionSplit(boolean insertExceptionSplit) {
        this.insertExceptionSplit = insertExceptionSplit;
    }
}
