/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.cloud.analyticdb.adb3client.impl;

import com.alibaba.cloud.analyticdb.adb3client.AdbConfig;
import com.alibaba.cloud.analyticdb.adb3client.model.Column;
import com.alibaba.cloud.analyticdb.adb3client.model.Record;
import com.alibaba.cloud.analyticdb.adb3client.model.TableName;
import com.alibaba.cloud.analyticdb.adb3client.model.TableSchema;
import com.alibaba.cloud.analyticdb.adb3client.model.WriteMode;
import com.alibaba.cloud.analyticdb.adb3client.util.IdentifierUtil;
import com.alibaba.cloud.analyticdb.adb3client.util.Tuple;
import com.alibaba.cloud.analyticdb.adb3client.util.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

/**
 * build upsert statement.
 */
public class UpsertStatementBuilder {
	public static final Logger LOGGER = LoggerFactory.getLogger(UpsertStatementBuilder.class);
	protected static final String DELIMITER_OR = " OR ";
	protected static final String DELIMITER_DOT = ", ";
	protected AdbConfig config;
	boolean enableDefaultValue;
	String defaultTimeStampText;

	boolean inputNumberAsEpochMsForDatetimeColumn;
	boolean inputStringAsEpochMsForDatetimeColumn;
	boolean removeU0000InTextColumnValue;

	public UpsertStatementBuilder(AdbConfig config) {
		this.config = config;
		this.enableDefaultValue = config.isEnableDefaultForNotNullColumn();
		this.defaultTimeStampText = config.getDefaultTimestampText();
		this.inputNumberAsEpochMsForDatetimeColumn = config.isInputNumberAsEpochMsForDatetimeColumn();
		this.inputStringAsEpochMsForDatetimeColumn = config.isInputStringAsEpochMsForDatetimeColumn();
		this.removeU0000InTextColumnValue = config.isRemoveU0000InTextColumnValue();
	}

	/**
	 * 表+record.isSet的列（BitSet） -> Sql语句的映射.
	 */
	static class SqlCache<T> {
		Map<Tuple3<TableSchema, TableName, WriteMode>, Map<T, String>> cacheMap = new HashMap<>();

		int size = 0;

		public String computeIfAbsent(Tuple3<TableSchema, TableName, WriteMode> tuple, T t, BiFunction<Tuple3<TableSchema, TableName, WriteMode>, T, String> b) {
			Map<T, String> subMap = cacheMap.computeIfAbsent(tuple, (s) -> new HashMap<>());
			return subMap.computeIfAbsent(t, (bs) -> {
				++size;
				return b.apply(tuple, bs);
			});
		}

		public int getSize() {
			return size;
		}

		public void clear() {
			cacheMap.clear();
		}
	}

	SqlCache<Tuple<BitSet, BitSet>> insertCache = new SqlCache<>();
	Map<Tuple<TableSchema, TableName>, String> deleteCache = new HashMap<>();
	boolean first = true;

	private String buildInsertSql(Tuple3<TableSchema, TableName, WriteMode> tuple, Tuple<BitSet, BitSet> input) {
		TableSchema schema = tuple.l;
		TableName tableName = tuple.m;
		WriteMode mode = tuple.r;
		BitSet set = input.l;
		BitSet onlyInsertSet = input.r;
		StringBuilder sb = new StringBuilder();
		if (WriteMode.INSERT_OR_IGNORE == mode) {
			sb.append("insert ignore into ").append(tableName.getFullName());
		} else if (WriteMode.INSERT_OR_REPLACE == mode) {
			sb.append("replace into ").append(tableName.getFullName());
		} else {
			sb.append("insert into ").append(tableName.getFullName());
		}

		sb.append("(");
		first = true;
		set.stream().forEach((index) -> {
			if (!first) {
				sb.append(",");
			}
			first = false;
			sb.append(IdentifierUtil.quoteIdentifier(schema.getColumn(index).getName()));
		});
		sb.append(")");

		sb.append(" values ");
		sb.append("(");
		first = true;
		set.stream().forEach((index) -> {
			if (!first) {
				sb.append(",");
			}
			first = false;
			sb.append("?");
			Column column = schema.getColumn(index);
			if (Types.BIT == column.getType() && "bit".equals(column.getTypeName())) {
				sb.append("::bit(").append(column.getPrecision()).append(")");
			} else if (Types.OTHER == column.getType() && "varbit".equals(column.getTypeName())) {
				sb.append("::bit varying(").append(column.getPrecision()).append(")");
			}
		});
		sb.append(")");
		return sb.toString();
	}

	/**
	 * 把default值解析为值和类型.
	 * 例:
	 * text default '-'  ,  ["-",null]
	 * timestamptz default '2021-12-12 12:12:12.123'::timestamp , ["2021-12-12 12:12:12.123","timestamp"]
	 *
	 * @param defaultValue 建表时设置的default值
	 * @return 2元组，值，[类型]
	 */
	private static String[] handleDefaultValue(String defaultValue) {
		String[] ret = defaultValue.split("::");
		if (ret.length == 1) {
			String[] temp = new String[2];
			temp[0] = ret[0];
			temp[1] = null;
			ret = temp;
		}
		if (ret[0].startsWith("'") && ret[0].endsWith("'") && ret[0].length() > 1) {
			ret[0] = ret[0].substring(1, ret[0].length() - 1);
		}
		return ret;
	}

	private static final int WARN_SKIP_COUNT = 10000;
	long warnCount = WARN_SKIP_COUNT;

	private void logWarnSeldom(String s, Object... obj) {
		if (++warnCount > WARN_SKIP_COUNT) {
			LOGGER.warn(s, obj);
			warnCount = 0;
		}
	}

	private void fillDefaultValue(Record record, Column column, int i) {

		//当列为空并且not null时，尝试在客户端填充default值
		if (record.getObject(i) == null && !column.getAllowNull()) {
			//对于serial列不处理default值
			if (column.isSerial()) {
				return;
			}
			if (column.getDefaultValue() != null) {
				String[] defaultValuePair = handleDefaultValue(String.valueOf(column.getDefaultValue()));
				String defaultValue = defaultValuePair[0];
				switch (column.getType()) {
					case Types.INTEGER:
					case Types.BIGINT:
					case Types.SMALLINT:
						record.setObject(i, Long.parseLong(defaultValue));
						break;
					case Types.DOUBLE:
					case Types.FLOAT:
						record.setObject(i, Double.parseDouble(defaultValue));
						break;
					case Types.DECIMAL:
					case Types.NUMERIC:
						record.setObject(i, new BigDecimal(defaultValue));
						break;
					case Types.BOOLEAN:
					case Types.BIT:
						record.setObject(i, Boolean.valueOf(defaultValue));
						break;
					case Types.CHAR:
					case Types.VARCHAR:
						record.setObject(i, defaultValue);
						break;
					case Types.TIME_WITH_TIMEZONE:
					case Types.TIME:
						if ("now()".equalsIgnoreCase(defaultValue) || "current_timestamp".equalsIgnoreCase(defaultValue)) {
							record.setObject(i, new Time(System.currentTimeMillis()));
						} else {
							record.setObject(i, defaultValue);
						}
						break;
					case Types.TIMESTAMP:
					case Types.DATE:
						if ("now()".equalsIgnoreCase(defaultValue) || "current_timestamp".equalsIgnoreCase(defaultValue)) {
							record.setObject(i, new Date(System.currentTimeMillis()));
						} else {
							record.setObject(i, defaultValue);
						}
						break;
					default:
						logWarnSeldom("unsupported default type,{}({})", column.getType(), column.getTypeName());
				}
			} else if (enableDefaultValue) {
				switch (column.getType()) {
					case Types.INTEGER:
					case Types.BIGINT:
					case Types.SMALLINT:
					case Types.TINYINT:
						record.setObject(i, 0L);
						break;
					case Types.DOUBLE:
					case Types.FLOAT:
					case Types.REAL:
						record.setObject(i, 0D);
						break;
					case Types.DECIMAL:
					case Types.NUMERIC:
						record.setObject(i, BigDecimal.ZERO);
						break;
					case Types.BOOLEAN:
					case Types.BIT:
						record.setObject(i, false);
						break;
					case Types.CHAR:
					case Types.VARCHAR:
						record.setObject(i, "");
						break;
					case Types.TIME_WITH_TIMEZONE:
					case Types.TIME:
						if (defaultTimeStampText == null) {
							record.setObject(i, new Time(0L));
						} else {
							record.setObject(i, defaultTimeStampText);
						}
						break;
					case Types.TIMESTAMP:
					case Types.DATE:
						if (defaultTimeStampText == null) {
							record.setObject(i, new Date(0L));
						} else {
							record.setObject(i, defaultTimeStampText);
						}
						break;
					case Types.OTHER:
						if ("boolean".equalsIgnoreCase(column.getTypeName())) {
							record.setObject(i, false);
						} else if (column.getTypeName().toLowerCase(Locale.ROOT).startsWith("map")) {
							record.setObject(i, "{}");
						} else if (column.getTypeName().toLowerCase(Locale.ROOT).startsWith("array")) {
							record.setObject(i, "[]");
						} else {
							logWarnSeldom("unsupported default type,{}({})", column.getType(), column.getTypeName());
						}
						break;
					default:
						logWarnSeldom("unsupported default type,{}({})", column.getType(), column.getTypeName());
				}
			}
		}
	}

	/**
	 * 給所有非serial且没有set的列都set成null.
	 *
	 * @param record
	 */
	private void fillNotSetValue(Record record, Column column, int i) {
		if (!record.isSet(i)) {
			if (column.isSerial()) {
				return;
			}
			record.setObject(i, null);
		}
	}

	private void handleArrayColumn(Connection conn, Record record, Column column, int index) throws SQLException {
		Object obj = record.getObject(index);
		if (null != obj && obj instanceof List) {
			List<?> list = (List<?>) obj;
			Array array = conn.createArrayOf(column.getTypeName().substring(1), list.toArray());
			record.setObject(index, array);
		} else if (obj != null && obj instanceof Object[]) {
			Array array = conn.createArrayOf(column.getTypeName().substring(1), (Object[]) obj);
			record.setObject(index, array);
		}

	}

	public void prepareRecord(Connection conn, Record record, WriteMode mode) throws SQLException {
		try {
			for (int i = 0; i < record.getSize(); ++i) {
				Column column = record.getSchema().getColumn(i);
				fillDefaultValue(record, column, i);
				fillNotSetValue(record, column, i);
				if (column.getType() == Types.ARRAY) {
					handleArrayColumn(conn, record, column, i);
				}
			}
		} catch (Exception e) {
			throw new SQLException("invalid record", e);
		}
	}

	private String removeU0000(final String in) {
		if (in != null && in.contains("\u0000")) {
			return in.replaceAll("\u0000", "");
		} else {
			return in;
		}
	}

	private static final String MYSQL_0000 = "0000-00-00 00:00:00";

	private void fillPreparedStatement(PreparedStatement ps, int index, Object obj, Column column) throws SQLException {
		switch (column.getType()) {
			case Types.OTHER:
				if ("boolean".equalsIgnoreCase(column.getTypeName())) {
					if (obj instanceof Boolean) {
						ps.setBoolean(index, (Boolean) obj);
					} else {
						ps.setString(index, obj == null ? null : String.valueOf(obj));
					}
				} else if (obj instanceof String) {
					ps.setString(index, (String) obj);
				} else {
					ps.setObject(index, obj, column.getType());
				}
				break;
			case Types.LONGNVARCHAR:
			case Types.VARCHAR:
			case Types.CHAR:
				if (obj == null) {
					ps.setNull(index, column.getType());
				} else {
					ps.setObject(index, removeU0000(obj.toString()), column.getType());
				}
				break;
			case Types.BIT:
				if ("bit".equals(column.getTypeName())) {
					if (obj instanceof Boolean) {
						ps.setString(index, (Boolean) obj ? "1" : "0");
					} else {
						ps.setString(index, obj == null ? null : String.valueOf(obj));
					}
				} else {
					ps.setObject(index, obj, column.getType());
				}
				break;
			case Types.TIMESTAMP_WITH_TIMEZONE:
			case Types.TIMESTAMP:
				if (obj instanceof Number && inputNumberAsEpochMsForDatetimeColumn) {
					ps.setObject(index, new Timestamp(((Number) obj).longValue()), column.getType());
				} else if (obj instanceof String && inputStringAsEpochMsForDatetimeColumn) {
					long l = 0L;
					try {
						l = Long.parseLong((String) obj);
						ps.setObject(index, new Timestamp(l), column.getType());
					} catch (NumberFormatException e) {
						if (MYSQL_0000.equals(obj)) {
							ps.setObject(index, new Timestamp(0), column.getType());
						} else {
							ps.setObject(index, obj, column.getType());
						}
					}
				} else {
					if (MYSQL_0000.equals(obj)) {
						ps.setObject(index, new Timestamp(0), column.getType());
					} else {
						ps.setObject(index, obj, column.getType());
					}
				}
				break;
			case Types.DATE:
				if (obj instanceof Number && inputNumberAsEpochMsForDatetimeColumn) {
					ps.setObject(index, new java.sql.Date(((Number) obj).longValue()), column.getType());
				} else if (obj instanceof String && inputStringAsEpochMsForDatetimeColumn) {
					long l = 0L;
					try {
						l = Long.parseLong((String) obj);
						ps.setObject(index, new java.sql.Date(l), column.getType());
					} catch (NumberFormatException e) {
						if (MYSQL_0000.equals(obj)) {
							ps.setObject(index, new java.sql.Date(0), column.getType());
						} else {
							ps.setObject(index, obj, column.getType());
						}
					}
				} else {
					if (MYSQL_0000.equals(obj)) {
						ps.setObject(index, new java.sql.Date(0), column.getType());
					} else {
						ps.setObject(index, obj, column.getType());
					}
				}
				break;
			case Types.TIME_WITH_TIMEZONE:
			case Types.TIME:
				if (obj instanceof Number && inputNumberAsEpochMsForDatetimeColumn) {
					ps.setObject(index, new Time(((Number) obj).longValue()), column.getType());
				} else if (obj instanceof String && inputStringAsEpochMsForDatetimeColumn) {
					long l = 0L;
					try {
						l = Long.parseLong((String) obj);
						ps.setObject(index, new Time(l), column.getType());
					} catch (NumberFormatException e) {
						if (MYSQL_0000.equals(obj)) {
							ps.setObject(index, new Time(0), column.getType());
						} else {
							ps.setObject(index, obj, column.getType());
						}
					}
				} else {
					if (MYSQL_0000.equals(obj)) {
						ps.setObject(index, new Time(0), column.getType());
					} else {
						ps.setObject(index, obj, column.getType());
					}
				}
				break;
			default:
				ps.setObject(index, obj, column.getType());
		}
	}

	private int fillPreparedStatementForInsert(PreparedStatement ps, int psIndex, Record record) throws SQLException {
		IntStream columnStream = record.getBitSet().stream();
		for (PrimitiveIterator.OfInt it = columnStream.iterator(); it.hasNext(); ) {
			int index = it.next();
			Column column = record.getSchema().getColumn(index);
			++psIndex;
			fillPreparedStatement(ps, psIndex, record.getObject(index), column);
		}
		return psIndex;
	}

	protected void buildInsertStatement(Connection conn, TableSchema schema, TableName tableName, Tuple<BitSet, BitSet> columnSet, List<Record> recordList, List<PreparedStatementWithBatchInfo> list, WriteMode mode) throws SQLException {
		if (recordList.size() == 0) {
			return;
		}
		String sql = insertCache.computeIfAbsent(new Tuple3<>(schema, tableName, mode), columnSet, this::buildInsertSql);
		fillPreparedStatement(conn, sql, list, recordList);
	}

	private void fillPreparedStatement(Connection conn, String sqlTemplate, List<PreparedStatementWithBatchInfo> list, List<Record> recordList) throws SQLException {
		PreparedStatementWithBatchInfo ps = new PreparedStatementWithBatchInfo(conn.prepareStatement(sqlTemplate), recordList.size() > 1);
		list.add(ps);
		boolean batchMode = recordList.size() > 1;
		long byteSize = 0L;
		for (Record record : recordList) {
			fillPreparedStatementForInsert(ps.l, 0, record);
			byteSize += record.getByteSize();
			if (batchMode) {
				ps.l.addBatch();
			}
		}
		if (ps != null) {
			ps.setByteSize(byteSize);
			ps.setBatchCount(recordList.size());
		}
	}

	/**
	 * @param conn
	 * @param recordList 必须都是同一张表的！！！！
	 * @param mode
	 * @return
	 * @throws SQLException
	 */
	public List<PreparedStatementWithBatchInfo> buildStatements(Connection conn, TableSchema schema, TableName tableName, Collection<Record> recordList,
																WriteMode mode) throws
			SQLException {
		Map<Tuple<BitSet, BitSet>, List<Record>> insertRecordList = new HashMap<>();
		List<PreparedStatementWithBatchInfo> preparedStatementList = new ArrayList<>();
		try {
			for (Record record : recordList) {
				prepareRecord(conn, record, mode);
				insertRecordList.computeIfAbsent(new Tuple<>(record.getBitSet(), record.getOnlyInsertColumnSet()), t -> new ArrayList<>()).add(record);
			}

			try {
				for (Map.Entry<Tuple<BitSet, BitSet>, List<Record>> entry : insertRecordList.entrySet()) {
					buildInsertStatement(conn, schema, tableName, entry.getKey(), entry.getValue(), preparedStatementList, mode);
				}
			} catch (SQLException e) {
				for (PreparedStatementWithBatchInfo psWithInfo : preparedStatementList) {
					PreparedStatement ps = psWithInfo.l;
					if (null != ps) {
						try {
							ps.close();
						} catch (SQLException e1) {

						}
					}

				}
				throw e;
			}
			return preparedStatementList;
		} catch (SQLException e) {
			throw e;
		} catch (Exception e) {
			throw new SQLException(e);
		} finally {
			if (insertCache.getSize() > 500) {
				insertCache.clear();
			}
			if (deleteCache.size() > 500) {
				deleteCache.clear();
			}
		}
	}
}
