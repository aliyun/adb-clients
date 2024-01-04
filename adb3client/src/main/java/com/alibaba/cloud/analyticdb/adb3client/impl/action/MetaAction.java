/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.cloud.analyticdb.adb3client.impl.action;

import com.alibaba.cloud.analyticdb.adb3client.model.TableName;
import com.alibaba.cloud.analyticdb.adb3client.model.TableSchema;

/**
 * ma.
 */
public class MetaAction extends AbstractAction<TableSchema> {

	TableName tableName;

	/**
	 * @param tableName 表名
	 */
	public MetaAction(TableName tableName) {
		this.tableName = tableName;
	}

	public TableName getTableName() {
		return tableName;
	}
}
