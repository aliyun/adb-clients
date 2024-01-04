/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.cloud.analyticdb.adb3client.impl.handler;

import com.alibaba.cloud.analyticdb.adb3client.AdbConfig;
import com.alibaba.cloud.analyticdb.adb3client.exception.AdbClientException;
import com.alibaba.cloud.analyticdb.adb3client.impl.RetryableDbExecutor;
import com.alibaba.cloud.analyticdb.adb3client.impl.action.SqlAction;

/**
 * SqlAction处理类.
 */
public class SqlActionHandler extends ActionHandler<SqlAction> {

	private static final String NAME = "sql";

	private final RetryableDbExecutor connectionHolder;

	public SqlActionHandler(RetryableDbExecutor connectionHolder, AdbConfig config) {
		super(config);
		this.connectionHolder = connectionHolder;
	}

	@Override
	public void handle(SqlAction action) {
		try {
			action.getFuture().complete(connectionHolder.executeWithRetry((conn) -> action.getHandler().apply(conn)));
		} catch (AdbClientException e) {
			action.getFuture().completeExceptionally(e);
		}
	}
}
