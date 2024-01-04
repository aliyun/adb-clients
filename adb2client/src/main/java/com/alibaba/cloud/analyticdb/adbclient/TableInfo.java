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

import java.util.ArrayList;
import java.util.List;

/**
 * AnalyticDB table meta.<br>
 * <p>
 * select table_schema, table_name,comments <br>
 * from information_schema.tables <br>
 * where table_schema='alimama' and table_name='click_af' limit 1 <br>
 * </p>
 * <p>
 * select ordinal_position,column_name,data_type,type_name,column_comment <br>
 * from information_schema.columns <br>
 * where table_schema='db_name' and table_name='table_name' <br>
 * and is_deleted=0 <br>
 * order by ordinal_position limit 1000 <br>
 * </p>
 *
 * @since 1.0.0
 */
public class TableInfo {

    private String tableSchema;
    private String tableName;
    private List<ColumnInfo> columns;
    private String comments;
    private String tableType;

    private String updateType;
    private String partitionType;
    private String partitionColumn;
    private String subPartitionColumn;
    private int partitionCount;
    private List<String> primaryKeyColumns;

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("TableInfo [tableSchema=").append(tableSchema).append(", tableName=").append(tableName)
                .append(", columns=").append(columns).append(", comments=").append(comments).append(",updateType=").append(updateType)
                .append(",partitionType=").append(partitionType).append(",partitionColumn=").append(partitionColumn).append(",partitionCount=").append(partitionCount)
                .append(",subPartitionColumn=").append(subPartitionColumn).append(",primaryKeyColumns=").append(primaryKeyColumns).append("]");
        return builder.toString();
    }

    public String getTableSchema() {
        return tableSchema;
    }

    protected void setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
    }

    public String getTableName() {
        return tableName;
    }

    protected void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<ColumnInfo> getColumns() {
        return columns;
    }

    public List<String> getColumnsNames() {
        List<String> columnNames = new ArrayList<String>();
        for (ColumnInfo column : this.getColumns()) {
            columnNames.add(column.getName());
        }
        return columnNames;
    }

    protected void setColumns(List<ColumnInfo> columns) {
        this.columns = columns;
    }

    protected void setUpdateType(String updateType) {
        this.updateType = updateType;
    }

    protected void setPartitionType(String partitionType) {
        this.partitionType = partitionType;
    }

    public String getPartitionColumn() {
        return partitionColumn;
    }

    protected void setPartitionColumn(String partitionColumn) {
        this.partitionColumn = partitionColumn;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    protected void setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    public List<String> getPrimaryKeyColumns() {
        return primaryKeyColumns;
    }

    protected void setPrimaryKeyColumns(List<String> primaryKeyColumns) {
        this.primaryKeyColumns = primaryKeyColumns;
    }

    public String getSubPartitionColumn() {
        return subPartitionColumn;
    }

    protected void setSubPartitionColumn(String subPartitionColumn) {
        this.subPartitionColumn = subPartitionColumn;
    }

    public String getUpdateType() {
        return updateType;
    }
}
