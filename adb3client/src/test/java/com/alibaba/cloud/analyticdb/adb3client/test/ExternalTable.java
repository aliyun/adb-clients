package com.alibaba.cloud.analyticdb.adb3client.test;

import com.alibaba.cloud.analyticdb.adb3client.AdbClient;
import com.alibaba.cloud.analyticdb.adb3client.AdbConfig;
import com.alibaba.cloud.analyticdb.adb3client.exception.AdbClientException;
import com.alibaba.cloud.analyticdb.adb3client.model.TableSchema;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * A demo class for ADB MySQL OSS external table feature.
 * Demonstrates how to create an OSS external table and query data from it
 * using the Hive-compatible DDL syntax with LOCATION and STORED AS.
 */
public class ExternalTable {

	public static void main(String[] args) throws Exception {

		String url = System.getenv("url");
		String username = System.getenv("username");
		String password = System.getenv("password");

		// OSS path for the external table, e.g. oss://my-bucket/adb/person/
		String ossLocation = System.getenv("oss_location");

		Properties properties = new Properties();
		properties.put("user", username);
		properties.put("password", password);

		try (Connection conn = DriverManager.getConnection(url, properties)) {
			String[] preSqls = new String[]{
					"create external database if not exists adb_external_db",
					"drop table if exists adb_external_db.oss_external_t1",
					"create external table adb_external_db.oss_external_t1(" +
							"id int," +
							"name varchar(1023)," +
							"age int," +
							"score double" +
							") " +
							"row format delimited fields terminated by ',' " +
							"stored as textfile " +
							"location '" + ossLocation + "'",
					// Insert demo data into the external table
					"insert into adb_external_db.oss_external_t1 select 1, 'alice', 25, 90.5",
					"insert into adb_external_db.oss_external_t1 select 2, 'bob', 17, 75.0",
					"insert into adb_external_db.oss_external_t1 select 3, 'carol', 30, 88.0",
			};

			for (String sql : preSqls) {
				try (Statement stat = conn.createStatement()) {
					stat.execute(sql);
				}
			}

			AdbConfig config = new AdbConfig();
			config.setJdbcUrl(url);
			config.setUsername(username);
			config.setPassword(password);

			try (AdbClient client = new AdbClient(config)) {
				TableSchema schema = client.getTableSchema("adb_external_db.oss_external_t1");
				System.out.println(schema);

				// Query data from OSS external table via AdbClient
				System.out.println("select * from adb_external_db.oss_external_t1-----------------------------------");
				client.sql(connection -> {
					try (Statement stat = connection.createStatement()) {
						stat.execute("select * from adb_external_db.oss_external_t1");
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

				// Query with filter pushdown to OSS external table
				System.out.println("select with filter from adb_external_db.oss_external_t1-----------------------------------");
				client.sql(connection -> {
					try (Statement stat = connection.createStatement()) {
						stat.execute("select id, name, score from adb_external_db.oss_external_t1 where age > 18 order by id");
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
