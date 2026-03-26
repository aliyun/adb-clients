package com.alibaba.cloud.analyticdb.adb3client.test;

import com.alibaba.cloud.analyticdb.adb3client.AdbClient;
import com.alibaba.cloud.analyticdb.adb3client.AdbConfig;
import com.alibaba.cloud.analyticdb.adb3client.Put;
import com.alibaba.cloud.analyticdb.adb3client.exception.AdbClientException;
import com.alibaba.cloud.analyticdb.adb3client.model.ShardMode;
import com.alibaba.cloud.analyticdb.adb3client.model.TableName;
import com.alibaba.cloud.analyticdb.adb3client.model.TableSchema;
import com.alibaba.cloud.analyticdb.adb3client.model.WriteMode;
import com.alibaba.cloud.analyticdb.adb3client.util.IdentifierUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

/**
 * Demo：内部库建 stage（{@code DISTRIBUTE BY HASH(batch_id)}）+ 外部库建 lake（OSS external），再 PUT 写 stage、
 * {@code insert into lake select ... from stage where batch_id = ?} 入湖，最后按本批 {@code batch_id} 清理 stage。
 * <p>
 * 建表逻辑与 {@link Demo} 中 JDBC {@code Statement#execute} 串联 DDL 的方式一致。表名固定为
 * {@code adb_demo_internal.lake_stage_demo}、{@code adb_external_db.lake_target_demo}；批次号固定为每次运行新生成的 UUID。
 * <p>
 * 环境变量（仅此四项）：
 * <ul>
 *   <li>{@code url}：JDBC URL</li>
 *   <li>{@code user}：数据库用户名（写入 {@link java.util.Properties} 的 {@code user} 键）</li>
 *   <li>{@code password}：密码</li>
 *   <li>{@code oss_location}：OSS 路径，用于 {@code STORED AS PARQUET} 外表的 {@code LOCATION}；建议以 {@code /} 结尾，且目录下为与列定义一致的 Parquet 文件（参见
 *   <a href="https://help.aliyun.com/zh/analyticdb/analyticdb-for-mysql/developer-reference/create-external-table">CREATE EXTERNAL TABLE</a>）</li>
 * </ul>
 */
public class LakeTablePutDemo {

	private static final String INTERNAL_DB = "adb_demo_internal";
	private static final String EXTERNAL_DB = "adb_external_db";
	private static final String STAGE_TABLE_NAME = "lake_stage_demo";
	private static final String LAKE_TABLE_NAME = "lake_target_demo";
	private static final String DEFAULT_STAGE_QUALIFIED = INTERNAL_DB + "." + STAGE_TABLE_NAME;
	private static final String DEFAULT_LAKE_QUALIFIED = EXTERNAL_DB + "." + LAKE_TABLE_NAME;

	private static final String COL_ID = "id";
	private static final String COL_NAME = "name";
	private static final String COL_BATCH = "batch_id";

	public static void main(String[] args) throws Exception {
		String url = System.getenv("url");
		String user = System.getenv("user");
		String password = System.getenv("password");
		String ossLocation = System.getenv("oss_location");

		Objects.requireNonNull(url, "env url is required");
		Objects.requireNonNull(user, "env user is required");
		Objects.requireNonNull(password, "env password is required");
		Objects.requireNonNull(ossLocation, "env oss_location is required");

		String stageQualified = DEFAULT_STAGE_QUALIFIED;
		String lakeQualified = DEFAULT_LAKE_QUALIFIED;

		Properties properties = new Properties();
		properties.put("user", user);
		properties.put("password", password);

		try (Connection conn = DriverManager.getConnection(url, properties)) {
			prepareStageAndLakeTables(conn, ossLocation.trim(), stageQualified, lakeQualified);
		}

		String lakeFull = TableName.valueOf(lakeQualified).getFullName();
		String stageFull = TableName.valueOf(stageQualified).getFullName();
		String qId = IdentifierUtil.quoteIdentifier(COL_ID);
		String qName = IdentifierUtil.quoteIdentifier(COL_NAME);
		String qBatch = IdentifierUtil.quoteIdentifier(COL_BATCH);

		AdbConfig config = new AdbConfig();
		config.setJdbcUrl(url);
		config.setUsername(user);
		config.setPassword(password);
		config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
		config.setShardMode(ShardMode.DISTRIBUTE_KEY_HASH);

		try (AdbClient client = new AdbClient(config)) {
			TableSchema stageSchema = client.getTableSchema(stageQualified);
			Integer idIdx = stageSchema.getColumnIndex(COL_ID);
			Integer nameIdx = stageSchema.getColumnIndex(COL_NAME);
			Integer batchIdx = stageSchema.getColumnIndex(COL_BATCH);
			if (idIdx == null || nameIdx == null || batchIdx == null) {
				throw new IllegalStateException(
						"stage table must have columns: " + COL_ID + ", " + COL_NAME + ", " + COL_BATCH);
			}

			String batchId = UUID.randomUUID().toString();
			try {
				List<Put> puts = new ArrayList<>();
				for (int i = 1; i <= 3; i++) {
					Put put = new Put(stageSchema);
					put.setObject(idIdx, i);
					put.setObject(nameIdx, "row-" + i);
					put.setObject(batchIdx, batchId);
					puts.add(put);
				}
				client.put(puts);
				client.flush();
	
				String etlSql = String.format(
						"insert into %s (%s, %s) select %s, %s from %s where %s = ?",
						lakeFull, qId, qName, qId, qName, stageFull, qBatch);
	
				System.out.println("ETL: " + etlSql + "  batch_id=" + batchId);
				client.sql(connection -> runEtl(connection, etlSql, batchId)).get();
			} finally {
				String delSql = String.format("delete from %s where %s = ?", stageFull, qBatch);
				System.out.println("clean stage: " + delSql + "  batch_id=" + batchId);
				client.sql(connection -> runUpdate(connection, delSql, batchId)).get();
			}
		} catch (AdbClientException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 与 {@link Demo} 相同风格的 DDL 串联：内部库内表 stage（{@code DISTRIBUTE BY HASH(batch_id)}）+ 外部库 OSS lake（{@code STORED AS PARQUET}，无 {@code ROW FORMAT}）。
	 */
	static void prepareStageAndLakeTables(Connection conn, String ossLocation, String stageQualified, String lakeQualified)
			throws SQLException {
		String loc = ossLocation.replace("'", "''");
		List<String> preSqls = new ArrayList<>();
		String stageDb = schemaOfQualified(stageQualified);
		String lakeDb = schemaOfQualified(lakeQualified);
		if (stageDb != null) {
			preSqls.add("create database if not exists " + stageDb);
		}
		if (lakeDb != null) {
			preSqls.add("create external database if not exists " + lakeDb);
		}
		preSqls.add("drop table if exists " + stageQualified);
		preSqls.add(
				"create table " + stageQualified + "("
						+ "id int not null,"
						+ "name varchar(1023),"
						+ "batch_id varchar(36) not null,"
						+ "primary key(id,batch_id)"
						+ ") DISTRIBUTE BY HASH(batch_id)");
		preSqls.add("drop table if exists " + lakeQualified);
		preSqls.add(
				"create external table " + lakeQualified + "("
						+ "id int,"
						+ "name string"
						+ ") "
						+ "STORED AS PARQUET "
						+ "location '" + loc + "'");

		for (String sql : preSqls) {
			try (Statement stat = conn.createStatement()) {
				stat.execute(sql);
			}
		}
	}

	/** 解析 {@code db.table} 中的库名；无 schema 前缀时返回 {@code null}。 */
	private static String schemaOfQualified(String qualified) {
		int dot = qualified.indexOf('.');
		return dot > 0 ? qualified.substring(0, dot) : null;
	}

	private static Integer runEtl(Connection connection, String etlSql, String batchId) throws SQLException {
		try (PreparedStatement ps = connection.prepareStatement(etlSql)) {
			ps.setString(1, batchId);
			int n = ps.executeUpdate();
			System.out.println("ETL rows affected: " + n);
		}
		return 0;
	}

	private static Integer runUpdate(Connection connection, String sql, String batchId) throws SQLException {
		try (PreparedStatement ps = connection.prepareStatement(sql)) {
			ps.setString(1, batchId);
			int n = ps.executeUpdate();
			System.out.println("delete from stage rows: " + n);
		}
		return 0;
	}

	private static Integer printQuery(Connection connection, String sql) throws SQLException {
		try (Statement st = connection.createStatement()) {
			try (ResultSet rs = st.executeQuery(sql)) {
				int columnCount = rs.getMetaData().getColumnCount();
				StringBuilder sb = new StringBuilder();
				while (rs.next()) {
					for (int i = 0; i < columnCount; ++i) {
						sb.append(rs.getObject(i + 1)).append(",");
					}
					System.out.println(sb.toString());
					sb.setLength(0);
				}
			}
		}
		return 0;
	}
}
