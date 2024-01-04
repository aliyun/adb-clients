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

/**
 * AnalyticDB column meta.<br>
 * <p>
 * select ordinal_position,column_name,data_type,type_name,column_comment <br>
 * from information_schema.columns <br>
 * where table_schema='db_name' and table_name='table_name' <br>
 * and is_deleted=0 <br>
 * order by ordinal_position  <br>
 * </p>
 *
 * @since 1.0.0
 */
public class ColumnInfo {

    private int ordinal;
    private String name;
    private ColumnDataType dataType;
    private boolean isDeleted;
    private boolean isNullable;
    private String defaultValue;
    private String comment;

    protected void setOrdinal(int ordinal) {
        this.ordinal = ordinal;
    }

    public String getName() {
        return name;
    }

    protected void setName(String name) {
        this.name = name;
    }

    public ColumnDataType getDataType() {
        return dataType;
    }

    protected void setDataType(ColumnDataType dataType) {
        this.dataType = dataType;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public boolean isNullable() {
        return isNullable;
    }

    protected void setNullable(boolean nullable) {
        isNullable = nullable;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    protected void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ColumnInfo [ordinal=").append(ordinal).append(", name=").append(name).append(", dataType=")
                .append(dataType).append(", isNullable=").append(isNullable).append(", comment=").append(comment)
                .append("]");
        return builder.toString();
    }

}
