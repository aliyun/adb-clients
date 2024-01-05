/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.cloud.analyticdb.adb3client.impl.collector;

import com.alibaba.cloud.analyticdb.adb3client.AdbConfig;
import com.alibaba.cloud.analyticdb.adb3client.exception.AdbClientException;
import com.alibaba.cloud.analyticdb.adb3client.impl.ExecutionPool;
import com.alibaba.cloud.analyticdb.adb3client.model.Record;
import com.alibaba.cloud.analyticdb.adb3client.model.TableName;
import com.alibaba.cloud.analyticdb.adb3client.util.ExceptionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Action收集器.
 * 每一个AdbClient对应一个ActionCollector
 * ActionCollector
 * - TableCollector PutAction收集器（表级别）
 * - TableShardCollector PutAction收集器（shard级别，此shard和ADB的shard是2个概念，仅代表客户端侧的数据攒批的分区）
 */
public class ActionCollector {
	private static final Logger LOGGER = LoggerFactory.getLogger(ActionCollector.class);
	Map<TableName, TableCollector> map;

	private ReentrantReadWriteLock flushLock = new ReentrantReadWriteLock();
	final AdbConfig config;
	final ExecutionPool pool;

	public ActionCollector(AdbConfig config, ExecutionPool pool) {
		map = new ConcurrentHashMap<>();
		this.config = config;
		this.pool = pool;
	}

	public long getByteSize() {
		return map.values().stream().collect(Collectors.summingLong(TableCollector::getByteSize));
	}

	public void append(Record record) throws AdbClientException {
		flushLock.readLock().lock();
		try {
			TableCollector pairArray = map.computeIfAbsent(record.getTableName(), (tableName) -> new TableCollector(config, pool));
			pairArray.append(record);
			AdbClientException exception = lastException.getAndSet(null);
			if (null != exception) {
				throw exception;
			}
		} finally {
			flushLock.readLock().unlock();
		}
	}

	AtomicReference<AdbClientException> lastException = new AtomicReference<>(null);

	/**
	 * 该函数仅由ExecutionPool后台线程调用，因此不能抛出任何异常，要在后续的flush(internal=false)、append时再抛出.
	 */
	public void tryFlush() {
		flushLock.readLock().lock();
		try {
			for (Iterator<Map.Entry<TableName, TableCollector>> iter = map.entrySet().iterator(); iter.hasNext(); ) {
				TableCollector array = iter.next().getValue();
				try {
					array.flush(false);
				} catch (AdbClientException e) {
					LOGGER.error("try flush fail", e);
					lastException.accumulateAndGet(e, (lastOne, newOne) -> ExceptionUtil.merge(lastOne, newOne));
				}
			}
		} finally {
			flushLock.readLock().unlock();
		}
	}

	public void flush(boolean internal) throws AdbClientException {
		flushLock.writeLock().lock();
		try {
			AdbClientException exception = null;
			int doneCount = 0;
			AtomicInteger uncommittedActionCount = new AtomicInteger(0);
			boolean async = true;
			while (true) {
				doneCount = 0;
				uncommittedActionCount.set(0);
				for (Iterator<Map.Entry<TableName, TableCollector>> iter = map.entrySet().iterator(); iter.hasNext(); ) {
					TableCollector array = iter.next().getValue();
					try {
						if (array.flush(true, async, uncommittedActionCount)) {
							++doneCount;
						}
					} catch (AdbClientException e) {
						exception = ExceptionUtil.merge(exception, e);
					}
				}
				if (doneCount == map.size()) {
					break;
				}
				if (uncommittedActionCount.get() == 0) {
					async = false;
				}
			}
			if (exception != null) {
				lastException.accumulateAndGet(exception, (lastOne, newOne) -> ExceptionUtil.merge(lastOne, newOne));
			}

			if (!internal) {
				AdbClientException last = lastException.getAndSet(null);
				if (last != null) {
					throw last;
				}
			}
		} finally {
			flushLock.writeLock().unlock();
		}
	}
}
