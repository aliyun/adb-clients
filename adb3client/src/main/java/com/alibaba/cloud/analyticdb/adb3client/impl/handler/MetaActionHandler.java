/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.cloud.analyticdb.adb3client.impl.handler;

import com.alibaba.cloud.analyticdb.adb3client.AdbConfig;
import com.alibaba.cloud.analyticdb.adb3client.exception.AdbClientException;
import com.alibaba.cloud.analyticdb.adb3client.impl.RetryableDbExecutor;
import com.alibaba.cloud.analyticdb.adb3client.impl.action.MetaAction;
import com.alibaba.cloud.analyticdb.adb3client.model.Column;
import com.alibaba.cloud.analyticdb.adb3client.model.TableName;
import com.alibaba.cloud.analyticdb.adb3client.model.TableSchema;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * MetaAction处理类.
 */
public class MetaActionHandler extends ActionHandler<MetaAction> {

	private static final String NAME = "meta";
	private final AdbConfig config;
	private final RetryableDbExecutor dbExecutor;

	public MetaActionHandler(RetryableDbExecutor dbExecutor, AdbConfig config) {
		super(config);
		this.config = config;
		this.dbExecutor = dbExecutor;
	}

	@Override
	public void handle(MetaAction action) {
		try {
			action.getFuture().complete((TableSchema) dbExecutor.executeWithRetry(connection -> {
				return getTableSchema(connection, action.getTableName());
			}));
		} catch (AdbClientException e) {
			action.getFuture().completeExceptionally(e);
		}
	}

	private TableSchema getTableSchema(final Connection conn, final TableName tableName) throws SQLException {
		String[] columns = null;
		int[] types = null;
		String[] typeNames = null;

		List<String> primaryKeyList = new ArrayList<>();
		try (ResultSet rs = conn.getMetaData().getPrimaryKeys(tableName.getSchemaName(), null, tableName.getTableName())) {
			while (rs.next()) {
				primaryKeyList.add(rs.getString(4));
			}
		}
		List<Column> columnList = new ArrayList<>();
		try (ResultSet rs = conn.getMetaData().getColumns(tableName.getSchemaName(), null, tableName.getTableName(), "%")) {
			while (rs.next()) {
				Column column = new Column();
				column.setName(rs.getString(4));
				column.setType(rs.getInt(5));
				column.setTypeName(rs.getString(6));
				column.setPrecision(rs.getInt(7));
				column.setScale(rs.getInt(9));
				column.setAllowNull(rs.getInt(11) == 1);
				column.setComment(rs.getString(12));
				column.setDefaultValue(rs.getObject(13));
				column.setArrayType(column.getTypeName().startsWith("_"));
				column.setPrimaryKey(primaryKeyList.contains(column.getName()));
				columnList.add(column);
			}
		}

		TableSchema.Builder builder = new TableSchema.Builder();

		builder.setColumns(columnList);
		builder.setTableName(tableName);
		builder.setNotExist(false);
		TableSchema tableSchema = builder.build();
		tableSchema.calculateProperties();
		return tableSchema;
	}
}
