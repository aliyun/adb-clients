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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * A demo class.
 */
public class DefaultValue {
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
					"drop table if exists `default`",
					"create table `default`(id int not null," +
							"c1 boolean not null," +
							"c2 tinyint not null," +
							"c3 smallint not null," +
							"c4 bigint not null," +
							"c5 float not null," +
							"c6 double not null," +
							"c7 decimal not null," +
							"c8 varchar(256) not null," +
							"c9 binary," +
							"c10 date not null," +
							"c11 time not null," +
							"c12 datetime not null," +
							"c13 timestamp not null," +
							"c14 array<int> not null," +
							"c15 map<int, string> not null," +
							"c16 json," +
							"primary key(id))"
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
			config.setWriteMode(WriteMode.INSERT_OR_IGNORE);
			config.setEnableDefaultForNotNullColumn(true);
			try (AdbClient client = new AdbClient(config)) {
				TableSchema schema0 = client.getTableSchema("default");

				Put put = new Put(schema0);
				put.setObject(0, 1);
				client.put(put);
				client.flush();

				System.out.println("select * from `default`-----------------------------------");
				client.sql(connection -> {
					try (Statement stat = connection.createStatement()) {
						stat.execute("select * from `default`");
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
