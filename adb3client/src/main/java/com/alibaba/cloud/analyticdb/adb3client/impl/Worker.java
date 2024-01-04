/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.cloud.analyticdb.adb3client.impl;

import com.alibaba.cloud.analyticdb.adb3client.AdbConfig;
import com.alibaba.cloud.analyticdb.adb3client.exception.AdbClientException;
import com.alibaba.cloud.analyticdb.adb3client.exception.ExceptionCode;
import com.alibaba.cloud.analyticdb.adb3client.impl.action.AbstractAction;
import com.alibaba.cloud.analyticdb.adb3client.impl.action.MetaAction;
import com.alibaba.cloud.analyticdb.adb3client.impl.action.PutAction;
import com.alibaba.cloud.analyticdb.adb3client.impl.action.SqlAction;
import com.alibaba.cloud.analyticdb.adb3client.impl.handler.ActionHandler;
import com.alibaba.cloud.analyticdb.adb3client.impl.handler.MetaActionHandler;
import com.alibaba.cloud.analyticdb.adb3client.impl.handler.PutActionHandler;
import com.alibaba.cloud.analyticdb.adb3client.impl.handler.SqlActionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * worker.
 */
public class Worker implements Runnable {
	public static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);

	final RetryableDbExecutor dbExecutor;
	ObjectChan<AbstractAction> recordCollector = new ObjectChan<>();
	final AtomicBoolean started;
	final AdbConfig config;
	AtomicReference<Throwable> fatal = new AtomicReference<>(null);
	private final String name;
	Map<Class, ActionHandler> handlerMap = new HashMap<>();

	public Worker(AdbConfig config, DataSource dataSource, AtomicBoolean started, int index) {
		this(config, dataSource, started, index, false);
	}

	public Worker(AdbConfig config, DataSource dataSource, AtomicBoolean started, int index, boolean isFixed) {
		this.config = config;
		this.started = started;
		this.name = (isFixed ? "Fixed-" : "") + "Worker-" + index;
		this.dbExecutor = new RetryableDbExecutor(dataSource, config);
		handlerMap.put(MetaAction.class, new MetaActionHandler(dbExecutor, config));
		handlerMap.put(PutAction.class, new PutActionHandler(dbExecutor, config));
		handlerMap.put(SqlAction.class, new SqlActionHandler(dbExecutor, config));
	}

	public boolean offer(AbstractAction action) throws AdbClientException {
		if (fatal.get() != null) {
			throw new AdbClientException(ExceptionCode.INTERNAL_ERROR, "fatal", fatal.get());
		}
		if (action != null) {
			if (!started.get()) {
				throw new AdbClientException(ExceptionCode.ALREADY_CLOSE, "worker is close");
			}
			return this.recordCollector.set(action);
		} else {
			return false; //this.recordCollector.set(new EmptyAction());
		}
	}

	protected  <T extends AbstractAction> void handle(T action) throws AdbClientException {
		try {
			ActionHandler<T> handler = handlerMap.get(action.getClass());
			if (handler == null) {
				throw new AdbClientException(ExceptionCode.INTERNAL_ERROR, "Unknown action:" + action.getClass().getName());
			}
			handler.handle(action);
		} catch (Throwable e) {
			if (action.getFuture() != null && !action.getFuture().isDone()) {
				action.getFuture().completeExceptionally(e);
			}
			throw e;
		}
	}

	@Override
	public void run() {
		LOGGER.info("worker:{} start", this);
		while (started.get()) {
			try {
				AbstractAction action = recordCollector.get(2000L, TimeUnit.MILLISECONDS);
				/*
				 * 每个循环做2件事情：
				 * 1 有action就执行action
				 * 2 根据connectionMaxIdleMs释放空闲connection
				 * */
				if (null != action) {
					try {
						handle(action);
					} finally {
						recordCollector.clear();
						if (action.getSemaphore() != null) {
							action.getSemaphore().release();
						}

					}
				}
			} catch (Throwable e) {
				LOGGER.error("should not happen", e);
				fatal.set(e);
				break;
			}

		}
		LOGGER.info("worker:{} stop", this);
	}

	@Override
	public String toString() {
		return name;
	}
}
