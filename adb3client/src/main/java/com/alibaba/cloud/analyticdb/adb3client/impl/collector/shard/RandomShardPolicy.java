/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.cloud.analyticdb.adb3client.impl.collector.shard;

import com.alibaba.cloud.analyticdb.adb3client.model.Record;

import java.util.concurrent.ThreadLocalRandom;

/**
 * 使用distribution key去做shard,没有distribution key则随机.
 */
public class RandomShardPolicy implements ShardPolicy{

	private int threadCount;

	@Override
	public void init(int threadCount) {
		this.threadCount = threadCount;
	}

	@Override
	public int locate(Record record) {
		ThreadLocalRandom rand = ThreadLocalRandom.current();
		return rand.nextInt(0, threadCount);
	}
}
