/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.cloud.analyticdb.adb3client.impl.collector.shard;

import com.alibaba.cloud.analyticdb.adb3client.model.Record;
import com.alibaba.cloud.analyticdb.adb3client.util.ShardUtil;

import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 使用distribution key去做shard,没有distribution key则随机.
 */
public class DistributionKeyShardPolicy implements ShardPolicy{

	private int threadCount;

	@Override
	public void init(int threadCount) {
		this.threadCount = threadCount;
	}

	@Override
	public int locate(Record record) {
		long raw = ShardUtil.hashLong(record, record.getSchema().getDistributionKeyIndex());
		if (record.getSchema().getShardCount() < threadCount * 0.8) {//shards少，以增大并发为主要目标
			return (int)(raw % threadCount);
		} else {
			return (int)(raw % record.getSchema().getShardCount() % threadCount);
		}
	}
}
