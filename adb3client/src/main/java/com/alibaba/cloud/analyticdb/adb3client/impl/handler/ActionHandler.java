/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.cloud.analyticdb.adb3client.impl.handler;

import com.alibaba.cloud.analyticdb.adb3client.AdbConfig;

/**
 * Action处理类.
 *
 * @param <T> Action类型
 */
public abstract class ActionHandler<T> {

	protected static final String METRIC_COST_MS = "_cost_ms";

	public ActionHandler(AdbConfig config) {

	}

	public abstract void handle(T action);
}
