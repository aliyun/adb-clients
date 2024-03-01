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
public class IgnoreTimeFormatError {
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
					"drop table if exists IgnoreTimeFormatError",
					"create table IgnoreTimeFormatError(id int not null," +
							"c1 date," +
							"c2 time," +
							"c3 datetime," +
							"c4 timestamp," +
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

			//正确格式可以写进去
			try (AdbClient client = new AdbClient(config)) {
				TableSchema schema0 = client.getTableSchema("IgnoreTimeFormatError");

				List<Put> putList = new ArrayList<>();
				Put put = new Put(schema0);
				put.setObject(0, 1);
				put.setObject(1, "2021-11-12");
				put.setObject(2, "23:59:59");
				put.setObject(3, "2021-11-12 23:59:59");
				put.setObject(4, "2021-11-12 23:59:59");
				putList.add(put);
				Put put2 = new Put(schema0);
				put2.setObject(0, 2);
				put2.setObject(1, "2021-11-12");
				put2.setObject(2, "23:59:59");
				put2.setObject(3, "2021-11-12 23:59:59");
				put2.setObject(4, "2021-11-12 23:59:59");
				putList.add(put2);
				client.put(putList);
				client.flush();

			} catch (AdbClientException e) {
				e.printStackTrace();
			}

			//格式有问题，报错
			try (AdbClient client = new AdbClient(config)) {
				TableSchema schema0 = client.getTableSchema("IgnoreTimeFormatError");

				List<Put> putList = new ArrayList<>();
				Put put3 = new Put(schema0);
				put3.setObject(0, 3);
				put3.setObject(1, "2021-11-12");
				put3.setObject(2, "23:59:59");
				put3.setObject(3, "2021-11-12 23:59:59");
				put3.setObject(4, "2021-11-12 23:59:59");
				putList.add(put3);
				Put put4 = new Put(schema0);
				put4.setObject(0, 4);
				put4.setObject(1, "2021-11-12h");
				put4.setObject(2, "23:59:59:979");
				put4.setObject(3, "2021-11-12 23:59:59:979");
				put4.setObject(4, "2021-11-12 23:59:59:979");
				putList.add(put4);
				client.put(putList);
				client.flush();

			} catch (AdbClientException e) {
				e.printStackTrace();
			}


			//忽略格式错误，写入null
			config.setIgnoreTimeFormatError(true);
			try (AdbClient client = new AdbClient(config)) {
				TableSchema schema0 = client.getTableSchema("IgnoreTimeFormatError");

				List<Put> putList = new ArrayList<>();
				Put put5 = new Put(schema0);
				put5.setObject(0, 5);
				put5.setObject(1, "2021-11-12");
				put5.setObject(2, "23:59:59");
				put5.setObject(3, "2021-11-12 23:59:59");
				put5.setObject(4, "2021-11-12 23:59:59");
				putList.add(put5);
				Put put6 = new Put(schema0);
				put6.setObject(0, 6);
				put6.setObject(1, "2021-11-12h");
				put6.setObject(2, "23l:59:59");
				put6.setObject(3, "2021-11-12 23:59:59:979");
				put6.setObject(4, "2021-11-12 23:59:59:979");
				putList.add(put6);
				client.put(putList);
				client.flush();

				System.out.println("select * from IgnoreTimeFormatError-----------------------------------");
				client.sql(connection -> {
					try (Statement stat = connection.createStatement()) {
						stat.execute("select * from IgnoreTimeFormatError");
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
