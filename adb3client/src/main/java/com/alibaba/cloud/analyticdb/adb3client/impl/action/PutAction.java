/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.cloud.analyticdb.adb3client.impl.action;

import com.alibaba.cloud.analyticdb.adb3client.impl.collector.BatchState;
import com.alibaba.cloud.analyticdb.adb3client.model.Record;
import com.alibaba.cloud.analyticdb.adb3client.model.TableSchema;
import com.alibaba.cloud.analyticdb.adb3client.model.WriteMode;

import java.security.InvalidParameterException;
import java.util.List;

/**
 * pa.
 */
public class PutAction extends AbstractAction<Void> {

	final List<Record> recordList;
	final long byteSize;
	BatchState state;
	TableSchema schema;
	WriteMode writeMode;

	/**
	 * 提供的recordList必须都是相同tableSchema下的.
	 *
	 * @param recordList 要写入的记录
	 * @param byteSize 记录大小
	 * @param mode writemode
	 * @param state batch状态
	 */
	public PutAction(List<Record> recordList, long byteSize, WriteMode mode, BatchState state) {
		this.recordList = recordList;
		this.byteSize = byteSize;
		this.state = state;
		this.writeMode = mode;
		if (recordList.size() > 0) {
			schema = recordList.get(0).getSchema();
			for (Record record : recordList) {
				if (!record.getSchema().equals(schema)) {
					throw new InvalidParameterException("Records in PutAction must for the same table. the first table is " + schema.getTableNameObj().getFullName() + " but found another table " + record.getSchema().getTableNameObj().getFullName());
				}
			}
		} else {
			throw new InvalidParameterException("Empty records in PutAction is invalid");
		}
	}

	public List<Record> getRecordList() {
		return recordList;
	}

	public WriteMode getWriteMode() {
		return writeMode;
	}

	public long getByteSize() {
		return byteSize;
	}

	public BatchState getState() {
		return state;
	}

	public TableSchema getSchema() {
		return schema;
	}
}
