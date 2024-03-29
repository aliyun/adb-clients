/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.cloud.analyticdb.adb3client.impl.collector;

import com.alibaba.cloud.analyticdb.adb3client.AdbConfig;
import com.alibaba.cloud.analyticdb.adb3client.exception.AdbClientException;
import com.alibaba.cloud.analyticdb.adb3client.exception.AdbClientWithDetailsException;
import com.alibaba.cloud.analyticdb.adb3client.impl.ExecutionPool;
import com.alibaba.cloud.analyticdb.adb3client.impl.collector.shard.DistributionKeyShardPolicy;
import com.alibaba.cloud.analyticdb.adb3client.impl.collector.shard.RandomShardPolicy;
import com.alibaba.cloud.analyticdb.adb3client.impl.collector.shard.ShardPolicy;
import com.alibaba.cloud.analyticdb.adb3client.model.Record;
import static com.alibaba.cloud.analyticdb.adb3client.model.ShardMode.DISTRIBUTE_KEY_HASH;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * PutAction收集器（表级别）.
 * 每一个AdbClient对应一个ActionCollector
 * ActionCollector
 * - TableCollector PutAction收集器（表级别）
 * - TableShardCollector PutAction收集器（shard级别，此shard和ADB的shard是2个概念，仅代表客户端侧的数据攒批的分区）
 */
public class TableCollector {
	public static final Logger LOG = LoggerFactory.getLogger(TableCollector.class);

	private TableShardCollector[] pairArray;
	private final AdbConfig config;
	private final ExecutionPool pool;
	private final ShardPolicy shardPolicy;
	private final long recordSampleInterval; //nano
	private long lastSampleTime = 0L;

	public TableCollector(AdbConfig config, ExecutionPool pool) {
		this.config = config;
		this.pool = pool;
		ShardPolicy shardPolicy1 = new RandomShardPolicy();
		if (config.getShardMode() == DISTRIBUTE_KEY_HASH) {
			shardPolicy1 = new DistributionKeyShardPolicy();
		}
		this.shardPolicy = shardPolicy1;
		this.recordSampleInterval = config.getRecordSampleInterval() * 1000000L;
		initTableShardCollector(config.getWriteThreadSize());
	}

	private void initTableShardCollector(int size) {
		TableShardCollector[] newPairArray = new TableShardCollector[size];
		for (int i = 0; i < newPairArray.length; ++i) {
			newPairArray[i] = new TableShardCollector(config, pool, newPairArray.length);
		}
		pairArray = newPairArray;
		shardPolicy.init(size);
	}

	public void resize(int size) {
		if (pairArray.length != size) {
			initTableShardCollector(size);
		}
	}

	public long getByteSize() {
		return Arrays.stream(pairArray).collect(Collectors.summingLong(TableShardCollector::getByteSize));
	}

	public void append(Record record) throws AdbClientException {
		long nano = System.nanoTime();
		if (recordSampleInterval > 0 && nano - lastSampleTime > recordSampleInterval) {
			Object attachmentObj = null;
			try {
				if (record.getAttachmentList() != null && record.getAttachmentList().size() > 0) {
					attachmentObj = record.getAttachmentList().get(0);
				}
				LOG.info("sample data: table name={}, record={}, attachObj={}", record.getSchema().getTableNameObj(), record, attachmentObj);
			} catch (Exception e) {
				LOG.warn("sample data fail", e);
			}
			lastSampleTime = nano;
		}
		int index = shardPolicy.locate(record);
		pairArray[index].append(record);
	}

	public boolean flush(boolean force) throws AdbClientException {
		return flush(force, true);
	}

	public boolean flush(boolean force, boolean async) throws AdbClientException {
		return flush(force, async, null);
	}

	public boolean flush(boolean force, boolean async, AtomicInteger uncommittedActionCount) throws AdbClientException {
		AdbClientWithDetailsException exception = null;

		int doneCount = 0;

		for (TableShardCollector pair : pairArray) {
			try {
				doneCount += pair.flush(force, async, uncommittedActionCount) ? 1 : 0;
			} catch (AdbClientWithDetailsException e) {
				if (exception == null) {
					exception = e;
				} else {
					exception.merge(e);
				}
			}
		}

		if (exception != null) {
			throw exception;
		}
		return doneCount == pairArray.length;
	}

	public int getShardCount() {
		return pairArray.length;
	}
}
