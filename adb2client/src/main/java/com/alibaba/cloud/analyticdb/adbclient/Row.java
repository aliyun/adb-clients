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
 * AnalyticDB client Row type
 *
 * @author caihua
 */
public class Row {
    private List<Object> columnValues;

    public Row() {
        this.columnValues = new ArrayList<Object>();
    }

    public Row(int columnNum) {
        this.columnValues = new ArrayList<Object>(columnNum);
    }

    public void setColumn(int index, Object value) {
        columnValues.add(index, value);
    }

    public void updateColumn(int index, Object value) {
        columnValues.set(index, value);
    }

    public List<Object> getColumnValues() {
        return columnValues;
    }

    public void setColumnValues(List<Object> values) {
        this.columnValues = values;
    }

    public int length() {
        int length = 0;
        for (Object v : columnValues) {
            if (v != null) {
                length = length + v.toString().getBytes().length;
            }
        }
        return length;
    }
}
