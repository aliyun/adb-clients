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
import java.sql.Statement;
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

		String distributeColumns = "";
		String physicalSchema = "";
		int shardCount = 1;
		String schema = tableName.getSchemaName();
		if (schema == null || schema.isEmpty()) {
			schema = conn.getCatalog();
		}
		String sql = String.format("select * from information_schema.kepler_meta_tables where table_name = '%s' and table_schema = '%s'",
				tableName.getTableName(), schema);
		try (Statement st = conn.createStatement()) {
			try (ResultSet rs = st.executeQuery(sql)) {
				if (rs.next()) {
					String distributeType = rs.getString("distribute_type");
					if (distributeType.equalsIgnoreCase("hash")) {
						distributeColumns = rs.getString("distribute_column");
						if (distributeColumns.equalsIgnoreCase("__adb_auto_id__")) {
							distributeColumns = "";
						}
					}
					physicalSchema = rs.getString("physical_table_schema");
				}
			}

			if (!physicalSchema.isEmpty()) {
				sql = String.format("select count(*) from information_schema.kepler_meta_shards where shard_name like '%s__%%'", physicalSchema);

				try (ResultSet rs = st.executeQuery(sql)) {
					if (rs.next()) {
						shardCount = rs.getInt(1);
					}
				}
			}
		}


		TableSchema.Builder builder = new TableSchema.Builder();

		builder.setColumns(columnList);
		builder.setTableName(tableName);
		if (!distributeColumns.isEmpty()) {
			builder.setDistributionKeys(distributeColumns.split(","));
			builder.setShardCount(shardCount);
		}
		builder.setNotExist(false);
		TableSchema tableSchema = builder.build();
		tableSchema.calculateProperties();
		return tableSchema;
	}
}
