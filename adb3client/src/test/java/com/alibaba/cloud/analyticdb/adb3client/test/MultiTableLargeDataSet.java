package com.alibaba.cloud.analyticdb.adb3client.test;

import com.alibaba.cloud.analyticdb.adb3client.AdbClient;
import com.alibaba.cloud.analyticdb.adb3client.AdbConfig;
import com.alibaba.cloud.analyticdb.adb3client.Put;
import com.alibaba.cloud.analyticdb.adb3client.exception.AdbClientException;
import com.alibaba.cloud.analyticdb.adb3client.model.TableSchema;
import com.alibaba.cloud.analyticdb.adb3client.model.WriteMode;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.Random;

/**
 * A demo class.
 */
public class MultiTableLargeDataSet {
	private static final int EVENT_COUNT = 10_000_0000;
	public static void main(String[] args) throws Exception {

		String url = System.getenv("url");
		String username = System.getenv("username");
		String password = System.getenv("password");

		Properties properties = new Properties();
		properties.put("user", username);
		properties.put("password", password);
		//prepare table
		try (Connection conn = DriverManager.getConnection(url, properties)) {
			String[] preSqls = new String[]{
					"drop table if exists large%d",
					"create table large%d(id int not null," +
							"c1 boolean," +
							"c2 tinyint," +
							"c3 smallint," +
							"c4 bigint," +
							"c5 float," +
							"c6 double," +
							"c7 decimal," +
							"c8 varchar(256)," +
							"c9 binary," +
							"c10 date," +
							"c11 time," +
							"c12 datetime," +
							"c13 timestamp," +
							"c14 array<int>," +
							"c15 map<int, string>," +
							"c16 json," +
							"primary key(id))"
			};

			for (int i = 0; i < 40; i++) {
				for (String sql : preSqls) {
					try (Statement stat = conn.createStatement()) {
						stat.execute(String.format(sql, i));
					}
				}
			}

			AdbConfig config = new AdbConfig();
			config.setJdbcUrl(url);
			config.setUsername(username);
			config.setPassword(password);
			config.setWriteMode(WriteMode.INSERT_OR_REPLACE);
			config.setWriteThreadSize(16);
			config.setWriteBatchSize(4096);
			config.setWriteBatchTotalByteSize(400 * 1024 * 1024);
			config.setWriteMaxIntervalMs(30000);
			// config.setEnableEarlyCommit(false);
			Random random = new Random();
			try (AdbClient client = new AdbClient(config)) {
				TableSchema schema0 = client.getTableSchema("large0");

				long start = System.currentTimeMillis();
				Put put = new Put(schema0);
				put.setObject(0, 1);
				put.setObject(1, false);
				put.setObject(2, 1);
				put.setObject(3, 1);
				put.setObject(4, 1);
				put.setObject(5, 1.2);
				put.setObject(6, 1.3);
				put.setObject(7, 1.4);
				put.setObject(8, "varchar");
				put.setObject(9, "0x0102");
				put.setObject(10, "2021-11-12");
				put.setObject(11, "23:59:59");
				put.setObject(12, new Timestamp(System.currentTimeMillis()));
				put.setObject(13, new Timestamp(System.currentTimeMillis()));
				put.setObject(14, "[1,2,3]");
				put.setObject(15, "{1:'a'}");
				put.setObject(16, "{\"id\":0, \"name\":\"abc\", \"age\":0}");
				client.put(put);
				for (int j = 1; j < EVENT_COUNT; j++) {
					int eventId = random.nextInt(40);
					TableSchema schema = client.getTableSchema("large" + eventId);
					Put put2 = new Put(schema);
					put2.setObject(0, j+1);
					for (int i = 1; i <= 16; i++) {
						if (put.isSet(i)) {
							put2.setObject(i, put.getObject(i));
						}
					}
					client.put(put2);
				}
				client.flush();
				System.out.println(String.format("Finished put %d record in %dms", EVENT_COUNT, System.currentTimeMillis() - start));

				System.out.println("select count(*) from large5-----------------------------------");
				client.sql(connection -> {
					try (Statement stat = connection.createStatement()) {
						stat.execute("select count(*) from large5");
						ResultSet rs = stat.getResultSet();
						int columnCount = rs.getMetaData().getColumnCount();
						StringBuilder sb = new StringBuilder();
						while (rs.next()) {
							for (int i = 0; i < columnCount; ++i) {
								sb.append(rs.getObject(i + 1)).append(",");
							}
							System.out.println(sb.toString());
							sb.setLength(0);
						}
						rs.close();
					}
					return 0;
				}).get();

			} catch (AdbClientException e) {
				e.printStackTrace();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
