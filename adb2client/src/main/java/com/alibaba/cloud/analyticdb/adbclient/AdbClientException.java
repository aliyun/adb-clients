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

import java.util.List;

/**
 * AnalyticDB client Exception <br>
 *
 * @author chase
 */
public class AdbClientException extends RuntimeException {
    private static final long serialVersionUID = 1080618043484079703L;


    public final static int SQL_LENGTH_LIMIT = 100;
    public final static int COMMIT_ERROR_DATA_LIST = 101;
    public final static int COMMIT_ERROR_OTHER = 102;
    public final static int ADD_DATA_ERROR = 103;
    public final static int CREATE_CONNECTION_ERROR = 104;
    public final static int CLOSE_CONNECTION_ERROR = 105;
    public final static int CONFIG_ERROR = 106;
    public final static int STOP_ERROR = 107;

    public final static int OTHER = 999;


    private int code = OTHER;
    private String message;
    private List<String> errData;

    public AdbClientException(int code, String message, Throwable e) {
        super(message, e);
        this.code = code;
        this.message = message;
    }

    public AdbClientException(int code, List<String> errData, Throwable e) {
        super(errData.toString(), e);
        this.code = code;
        this.errData = errData;
        if (e != null) {
            this.message = e.getMessage();
        }
    }

    @Override
    public String getMessage() {
        if (errData != null) {
            return "Code=" + this.code + " Message=" + this.message + ". Error data=" + errData.toString();
        } else {
            return "Code=" + this.code + " Message=" + this.message;
        }
    }

    public int getCode() {
        return this.code;
    }

    public List<String> getErrData() {
        return errData;
    }
}
