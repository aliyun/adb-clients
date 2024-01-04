/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.cloud.analyticdb.adb3client.impl;

import com.alibaba.cloud.analyticdb.adb3client.Put;
import com.alibaba.cloud.analyticdb.adb3client.util.Tuple;

import java.sql.PreparedStatement;

/**
 * preparedStatement，是否需要batchExecute.
 */
public class PreparedStatementWithBatchInfo extends Tuple<PreparedStatement, Boolean> {
	long byteSize;
	int batchCount;
	public PreparedStatementWithBatchInfo(PreparedStatement preparedStatement, Boolean isBatch) {
		super(preparedStatement, isBatch);
	}

	public int getBatchCount() {
		return batchCount;
	}

	public void setBatchCount(int batchCount) {
		this.batchCount = batchCount;
	}

	public long getByteSize() {
		return byteSize;
	}

	public void setByteSize(long byteSize) {
		this.byteSize = byteSize;
	}
}
