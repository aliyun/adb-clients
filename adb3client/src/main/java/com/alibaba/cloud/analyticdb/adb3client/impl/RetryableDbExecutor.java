package com.alibaba.cloud.analyticdb.adb3client.impl;

import com.alibaba.cloud.analyticdb.adb3client.AdbConfig;
import com.alibaba.cloud.analyticdb.adb3client.exception.AdbClientException;
import com.alibaba.cloud.analyticdb.adb3client.exception.ExceptionCode;
import com.alibaba.cloud.analyticdb.adb3client.function.FunctionWithSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class RetryableDbExecutor {
	public static final Logger LOG = LoggerFactory.getLogger(RetryableDbExecutor.class);
	private final DataSource datasource;
	private final int retryCount;
	final long retrySleepStepMs;
	final long retrySleepInitMs;

	public RetryableDbExecutor(DataSource dataSource, final AdbConfig config) {
		this.datasource = dataSource;
		this.retryCount = config.getRetryCount();
		this.retrySleepInitMs = config.getRetrySleepInitMs();
		this.retrySleepStepMs = config.getRetrySleepStepMs();
	}

	public synchronized <T> T executeWithRetry(FunctionWithSQLException<Connection, T> action) throws AdbClientException {
		AdbClientException e = null;
		for (int i = 0; i < retryCount; ++i) {
			try(Connection conn = datasource.getConnection()) {
				return action.apply(conn);
			} catch (SQLException exception) {
				e = AdbClientException.fromSqlException(exception);
				if (i == retryCount - 1 || !needRetry(e)) {
					throw e;
				} else {
					long sleepTime = retrySleepStepMs * i + retrySleepInitMs;
					LOG.warn("execute sql fail, try again[" + (i + 1) + "/" + retryCount + "], sleepMs = " + sleepTime + " ms", exception);
					try {
						Thread.sleep(sleepTime);
					} catch (InterruptedException ignore) {

					}
				}
			} catch (Exception exception) {
				throw new AdbClientException(ExceptionCode.INTERNAL_ERROR, "execute fail", exception);
			}
		}
		throw e;
	}

	private boolean needRetry(AdbClientException e) {
		if (e.getCode() == ExceptionCode.AUTH_FAIL) {
			return false;
		}
		return true;
	}
}
