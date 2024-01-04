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

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * AnalyticDB column data type. <br>
 *
 * @since 1.0.0
 */
public class ColumnDataType {

    public static final int BOOLEAN = 1;
    public static final int BYTE = 2;
    public static final int SHORT = 3;
    public static final int INT = 4;
    public static final int LONG = 5;
    public static final int DECIMAL = 6;
    public static final int DOUBLE = 7;
    public static final int FLOAT = 8;
    public static final int TIME = 9;
    public static final int DATE = 10;
    public static final int TIMESTAMP = 11;
    public static final int STRING = 13;
    // public static final int STRING_IGNORECASE = 14;
    public static final int BLOB = 15;
    public static final int CLOB = 16;
    //public static final int DATETIME = 17;
    public static final int JSON = 18;
    public static final int ARRAY = 19;
    public static final int ARRAY_BOOLEAN = 50;
    public static final int ARRAY_BYTE = 51;
    public static final int ARRAY_SHORT = 52;
    public static final int ARRAY_INT = 53;
    public static final int ARRAY_FLOAT = 54;
    public static final int ARRAY_DOUBLE = 55;
    public static final int ARRAY_LONG = 56;

    // public static final int STRING_FIXED = 21;

    public static final int MULTI_VALUE = 22;

    public static final int TYPE_COUNT = ARRAY_LONG + 1;

    /**
     * The list of types. An ArrayList so that Tomcat doesn't set it to null when clearing references.
     */
    private static final ArrayList<ColumnDataType> TYPES = new ArrayList<ColumnDataType>();
    private static final HashMap<String, ColumnDataType> TYPES_BY_NAME = new HashMap<String, ColumnDataType>();
    private static final ArrayList<ColumnDataType> TYPES_BY_VALUE_TYPE = new ArrayList<ColumnDataType>();

    /**
     * @param dataTypes int[]
     * @return String
     */
    public static String getNames(int[] dataTypes) {
        List<String> names = new ArrayList<String>(dataTypes.length);
        for (final int dataType : dataTypes) {
            names.add(ColumnDataType.getDataType(dataType).name);
        }
        return names.toString();
    }

    protected int type;
    protected String name;
    protected int sqlType;
    protected String jdbc;

    /**
     * How closely the data type maps to the corresponding JDBC SQL type (low is best).
     */
    protected int sqlTypePos;

    static {
        for (int i = 0; i < TYPE_COUNT; i++) {
            TYPES_BY_VALUE_TYPE.add(null);
        }
        // add(NULL, Types.NULL, "Null", new String[] { "NULL" });
        add(STRING, Types.VARCHAR, "String", new String[]{"VARCHAR", "VARCHAR2", "NVARCHAR", "NVARCHAR2",
                "VARCHAR_CASESENSITIVE", "CHARACTER VARYING", "TID"});
        add(STRING, Types.LONGVARCHAR, "String", new String[]{"LONGVARCHAR", "LONGNVARCHAR"});
        // add(STRING_FIXED, Types.CHAR, "String", new String[] { "CHAR", "CHARACTER", "NCHAR" });
        // add(STRING_IGNORECASE, Types.VARCHAR, "String", new String[] { "VARCHAR_IGNORECASE" });
        add(BOOLEAN, Types.BOOLEAN, "Boolean", new String[]{"BOOLEAN", "BIT", "BOOL"});
        add(BYTE, Types.TINYINT, "Byte", new String[]{"TINYINT"});
        add(SHORT, Types.SMALLINT, "Short", new String[]{"SMALLINT", "YEAR", "INT2"});
        add(INT, Types.INTEGER, "Int", new String[]{"INTEGER", "INT", "MEDIUMINT", "INT4", "SIGNED"});
        add(INT, Types.INTEGER, "Int", new String[]{"SERIAL"});
        add(LONG, Types.BIGINT, "Long", new String[]{"BIGINT", "INT8", "LONG"});
        add(LONG, Types.BIGINT, "Long", new String[]{"IDENTITY", "BIGSERIAL"});
        add(DECIMAL, Types.DECIMAL, "BigDecimal", new String[]{"DECIMAL", "DEC"});
        add(DECIMAL, Types.NUMERIC, "BigDecimal", new String[]{"NUMERIC", "NUMBER"});
        add(FLOAT, Types.REAL, "Float", new String[]{"REAL", "FLOAT4"});
        add(DOUBLE, Types.DOUBLE, "Double", new String[]{"DOUBLE", "DOUBLE PRECISION"});
        add(DOUBLE, Types.FLOAT, "Double", new String[]{"FLOAT", "FLOAT8"});
        add(TIME, Types.TIME, "Time", new String[]{"TIME"});
        add(DATE, Types.DATE, "Date", new String[]{"DATE"});
        add(TIMESTAMP, Types.TIMESTAMP, "Timestamp", new String[]{"TIMESTAMP", "DATETIME", "SMALLDATETIME"});
        add(MULTI_VALUE, Types.VARCHAR, "String", new String[]{"MULTIVALUE"});
        add(BLOB, Types.VARCHAR, "String", new String[]{"BLOB"});
        add(CLOB, Types.VARCHAR, "String", new String[]{"CLOB"});
        add(JSON, Types.VARCHAR, "String", new String[]{"JSON"});
        add(ARRAY, Types.VARCHAR, "String", new String[]{"ARRAY"});
        add(ARRAY_BOOLEAN, Types.VARCHAR, "String", new String[]{"ARRAY_BOOLEAN"});
        add(ARRAY_BYTE, Types.VARCHAR, "String", new String[]{"ARRAY_BYTE"});
        add(ARRAY_SHORT, Types.VARCHAR, "String", new String[]{"ARRAY_SHORT"});
        add(ARRAY_INT, Types.VARCHAR, "String", new String[]{"ARRAY_INT"});
        add(ARRAY_FLOAT, Types.VARCHAR, "String", new String[]{"ARRAY_FLOAT"});
        add(ARRAY_DOUBLE, Types.VARCHAR, "String", new String[]{"ARRAY_DOUBLE"});
        add(ARRAY_LONG, Types.VARCHAR, "String", new String[]{"ARRAY_LONG"});
    }

    private static void add(int type, int sqlType, String jdbc, String[] names) {
        for (int i = 0; i < names.length; i++) {
            ColumnDataType dt = new ColumnDataType();
            dt.type = type;
            dt.sqlType = sqlType;
            dt.jdbc = jdbc;
            dt.name = names[i];
            for (ColumnDataType t2 : TYPES) {
                if (t2.sqlType == dt.sqlType) {
                    dt.sqlTypePos++;
                }
            }
            TYPES_BY_NAME.put(dt.name, dt);
            if (TYPES_BY_VALUE_TYPE.get(type) == null) {
                TYPES_BY_VALUE_TYPE.set(type, dt);
            }
            TYPES.add(dt);
        }
    }


    /**
     * Get the data type object for the given value type.
     *
     * @param type the value type
     * @return the data type object
     */
    public static ColumnDataType getDataType(int type) {
        if (type < 0 || type >= TYPE_COUNT) {
            throw new IllegalArgumentException("type=" + type);
        }
        ColumnDataType dt = TYPES_BY_VALUE_TYPE.get(type);
        return dt;
    }


    /**
     * Get a data type object from a type name.
     *
     * @param s the type name
     * @return the data type object
     */
    public static ColumnDataType getTypeByName(String s) {
        return TYPES_BY_NAME.get(s);
    }


}
