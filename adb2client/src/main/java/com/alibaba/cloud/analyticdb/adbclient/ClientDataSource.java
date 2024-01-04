package com.alibaba.cloud.analyticdb.adbclient;

import com.alibaba.druid.pool.DruidDataSource;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author chase
 */
public class ClientDataSource {
    private static ClientDataSource instance = null;
    private Map<String, DruidDataSource> dataSourceMap = new ConcurrentHashMap<String, DruidDataSource>();

    private ClientDataSource() {
    }

    public static ClientDataSource getInstance() {
        if (instance == null) {
            instance = new ClientDataSource();
        }
        return instance;
    }

    /**
     * Get dataSource
     *
     * @param databaseConfig DatabaseConfig
     * @return dataSource DruidDataSource
     */
    public synchronized DruidDataSource getDataSource(DatabaseConfig databaseConfig) {
        if (dataSourceMap.get(databaseConfig.getDatabase()) != null) {
            return dataSourceMap.get(databaseConfig.getDatabase());
        }
        DruidDataSource dataSource = newDataSource(databaseConfig);
        dataSourceMap.put(databaseConfig.getDatabase(), dataSource);
        return dataSource;
    }

    /**
     * New Datasource
     *
     * @param databaseConfig DatabaseConfig
     * @return dataSource DruidDataSource
     */
    public DruidDataSource newDataSource(DatabaseConfig databaseConfig) {
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setDriverClassName(com.mysql.jdbc.Driver.class.getName());
        dataSource.setUsername(databaseConfig.getUser());
        dataSource.setPassword(databaseConfig.getPassword());
        dataSource.setUrl("jdbc:mysql://" + databaseConfig.getHost() + ":" + databaseConfig.getPort() + "/" + databaseConfig.getDatabase());
        if (!databaseConfig.isShareDataSource()) {
            dataSource.setInitialSize(1);
            dataSource.setMinIdle(1);
        } else {
            dataSource.setInitialSize(4);
            dataSource.setMinIdle(4);
        }
        dataSource.setMaxActive(512);
        dataSource.setPoolPreparedStatements(false);
        dataSource.setValidationQuery("show status like '%Service_Status%'");
        dataSource.setValidationQueryTimeout(1000);
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        dataSource.setTestWhileIdle(true);
        dataSource.setTestOnBorrow(false);
        dataSource.setTestOnReturn(false);
        dataSource.setKeepAlive(true);
        dataSource.setPhyMaxUseCount(10000);
        return dataSource;
    }
}
