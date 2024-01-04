/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.cloud.analyticdb.adb3client.impl.collector.shard;

import com.alibaba.cloud.analyticdb.adb3client.model.Record;

/**
 * shard策略.
 */
public interface ShardPolicy {

	void init(int shardCount);

	int locate(Record record);

}
