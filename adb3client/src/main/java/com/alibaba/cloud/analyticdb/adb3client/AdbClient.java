/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.cloud.analyticdb.adb3client;

import com.alibaba.cloud.analyticdb.adb3client.exception.AdbClientException;
import com.alibaba.cloud.analyticdb.adb3client.exception.AdbClientWithDetailsException;
import com.alibaba.cloud.analyticdb.adb3client.exception.ExceptionCode;
import com.alibaba.cloud.analyticdb.adb3client.function.FunctionWithSQLException;
import com.alibaba.cloud.analyticdb.adb3client.impl.ExecutionPool;
import com.alibaba.cloud.analyticdb.adb3client.impl.action.PutAction;
import com.alibaba.cloud.analyticdb.adb3client.impl.action.SqlAction;
import com.alibaba.cloud.analyticdb.adb3client.impl.collector.ActionCollector;
import com.alibaba.cloud.analyticdb.adb3client.impl.collector.BatchState;
import com.alibaba.cloud.analyticdb.adb3client.model.Record;
import com.alibaba.cloud.analyticdb.adb3client.model.TableName;
import com.alibaba.cloud.analyticdb.adb3client.model.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 线程不安全，每个client，最多会创建2个JDBC connection，读写分离.
 */
public class AdbClient implements Closeable {

	public static final Logger LOGGER = LoggerFactory.getLogger(AdbClient.class);

	static {
		// Load DriverManager first to avoid deadlock between DriverManager's
		// static initialization block and specific driver class's static
		// initialization block when two different driver classes are loading
		// concurrently using Class.forName while DriverManager is uninitialized
		// before.
		//
		// This could happen in JDK 8 but not above as driver loading has been
		// moved out of DriverManager's static initialization block since JDK 9.
		DriverManager.getDrivers();
	}

	private ActionCollector collector;

	/**
	 * 是否使用fixed fe.
	 * 开启的话会创建fixed pool，用于执行点查、写入以及prefix scan.
	 */
	private final boolean useFixedFe;
	private ExecutionPool fixedPool = null;

	private ExecutionPool pool = null;
	private final AdbConfig config;

	/**
	 * 在AsyncCommit为true，调用put方法时，当记录数>=writeBatchSize 或 总记录字节数数>=writeBatchByteSize 调用flush进行提交.
	 * 否则每次调用put都会调用flush.
	 * 默认为true
	 *
	 * @HasGetter
	 * @HasSetter
	 */
	boolean asyncCommit = true;

	boolean isEmbeddedPool = false;
	boolean isEmbeddedFixedPool = false;

	public AdbClient(AdbConfig config) throws AdbClientException {
		try {
			DriverManager.getDrivers();
			Class.forName("com.mysql.jdbc.Driver");
		} catch (Exception e) {
			throw new AdbClientException(ExceptionCode.INTERNAL_ERROR, "load driver fail", e);
		}
		checkConfig(config);
		this.config = config;
		this.useFixedFe = config.isUseFixedFe();
	}

	private void checkConfig(AdbConfig config) throws AdbClientException {
		if (config.getJdbcUrl() == null || config.getJdbcUrl().isEmpty()) {
			throw new AdbClientException(ExceptionCode.INVALID_Config, "jdbcUrl cannot be null");
		}
		if (config.getPassword() == null || config.getPassword().isEmpty()) {
			throw new AdbClientException(ExceptionCode.INVALID_Config, "password cannot be null");
		}

		if (config.getUsername() == null || config.getUsername().isEmpty()) {
			throw new AdbClientException(ExceptionCode.INVALID_Config, "username cannot be null");
		}
		if (config.getWriteBatchSize() < 1) {
			throw new AdbClientException(ExceptionCode.INVALID_Config, "batchSize must > 0");
		}
		if (config.getWriteBatchByteSize() < 1) {
			throw new AdbClientException(ExceptionCode.INVALID_Config, "batchByteSize must > 0");
		}
	}

	public TableSchema getTableSchema(String tableName) throws AdbClientException {
		return getTableSchema(TableName.valueOf(tableName), false);
	}

	public TableSchema getTableSchema(String tableName, boolean noCache) throws AdbClientException {
		return getTableSchema(TableName.valueOf(tableName), noCache);
	}

	public TableSchema getTableSchema(TableName tableName) throws AdbClientException {
		return getTableSchema(tableName, false);
	}

	public TableSchema getTableSchema(TableName tableName, boolean noCache) throws AdbClientException {
		ensurePoolOpen();
		return pool.getOrSubmitTableSchema(tableName, noCache);
	}

	private void checkPut(Put put) throws AdbClientException {
		if (put == null) {
			throw new AdbClientException(ExceptionCode.CONSTRAINT_VIOLATION, "Put cannot be null");
		}
		for (int index : put.getRecord().getKeyIndex()) {
			if ((!put.getRecord().isSet(index) || null == put.getRecord().getObject(index)) && put.getRecord().getSchema().getColumn(index).getDefaultValue() == null) {
				throw new AdbClientWithDetailsException(ExceptionCode.CONSTRAINT_VIOLATION, "Put primary key cannot be null:" + put.getRecord().getSchema().getColumnSchema()[index].getName(), put.getRecord());
			}
		}
		if (put.getRecord().getSchema().isPartitionParentTable() && (!put.getRecord().isSet(put.getRecord().getSchema().getPartitionIndex()) || null == put.getRecord().getObject(put.getRecord().getSchema().getPartitionIndex()))) {
			throw new AdbClientWithDetailsException(ExceptionCode.CONSTRAINT_VIOLATION, "Put partition key cannot be null:" + put.getRecord().getSchema().getColumnSchema()[put.getRecord().getSchema().getPartitionIndex()].getName(), put.getRecord());

		}
	}

	public <T> CompletableFuture<T> sql(FunctionWithSQLException<Connection, T> func) throws AdbClientException {
		ensurePoolOpen();
		SqlAction<T> action = new SqlAction<>(func);
		while (!pool.submit(action)) {

		}
		return action.getFuture();
	}

	private void ensurePoolOpen() throws AdbClientException {
		if (pool == null) {
			synchronized (this) {
				if (pool == null) {
					ExecutionPool temp = new ExecutionPool("embedded-" + config.getAppName(), config, false);
					// 当useFixedFe为true 则不赋值新的collector，调用这个函数仅仅为了把pool标记为started.
					ActionCollector tempCollector = temp.register(this, config);
					if (!this.useFixedFe) {
						collector = tempCollector;
					}
					pool = temp;
					isEmbeddedPool = true;
				}
			}
		}
		if (!pool.isRunning()) {
			throw new AdbClientException(ExceptionCode.ALREADY_CLOSE,
					"already close at " + pool.getCloseReasonStack().l, pool.getCloseReasonStack().r);
		}
		if (useFixedFe && fixedPool == null) {
			synchronized (this) {
				if (fixedPool == null) {
					ExecutionPool temp = new ExecutionPool("embedded-fixed-" + config.getAppName(), config, true);
					// 当useFixedFe为true时, 只会使用这个fixedPool注册的collector.
					//collector = temp.register(this, config);
					fixedPool = temp;
					isEmbeddedFixedPool = true;
				}
			}
		}
		if (useFixedFe && !fixedPool.isRunning()) {
			throw new AdbClientException(ExceptionCode.ALREADY_CLOSE, "already close");
		}
	}

	public synchronized void setPool(ExecutionPool pool) throws AdbClientException {
		if (pool.isFixedPool()) {
			throw new AdbClientException(ExceptionCode.INTERNAL_ERROR, "fixed pool recived, require is not fixed");
		}
		ExecutionPool temp = pool;
		ActionCollector tempCollector = temp.register(this, config);
		if (!this.useFixedFe) {
			collector = tempCollector;
		}
		this.pool = temp;
		isEmbeddedPool = false;
	}

	public synchronized void setFixedPool(ExecutionPool fixedPool) throws AdbClientException {
		if (!useFixedFe || fixedPool == null || !fixedPool.isFixedPool()) {
			throw new AdbClientException(ExceptionCode.INTERNAL_ERROR, "fixedPool required not null, and enable AdbConfig:useFixedFe");
		}
		ExecutionPool tempFixedPool = fixedPool;
		this.fixedPool = tempFixedPool;
		collector = tempFixedPool.register(this, config);
		isEmbeddedFixedPool = false;
	}

	private void tryThrowException() throws AdbClientException {
		if (pool != null) {
			pool.tryThrowException();
		}
		if (fixedPool != null) {
			fixedPool.tryThrowException();
		}
	}

	public void put(Put put) throws AdbClientException {
		ensurePoolOpen();
		tryThrowException();
		checkPut(put);
		ExecutionPool execPool = useFixedFe ? fixedPool : pool;
		if (!asyncCommit) {
			Record r = put.getRecord();
			PutAction action = new PutAction(Collections.singletonList(r), r.getByteSize(), config.getWriteMode(), BatchState.SizeEnough);
			while (!execPool.submit(action)) {

			}
			action.getResult();
		} else {
			collector.append(put.getRecord());
		}
	}

	public CompletableFuture<Void> putAsync(Put put) throws AdbClientException {
		CompletableFuture<Void> ret = new CompletableFuture<>();
		ensurePoolOpen();
		tryThrowException();
		checkPut(put);
		put.getRecord().setPutFuture(ret);
		collector.append(put.getRecord());
		return ret;
	}

	public void put(List<Put> puts) throws AdbClientException {
		ensurePoolOpen();
		tryThrowException();
		AdbClientWithDetailsException detailException = null;
		List<Put> putList = new ArrayList<>();
		for (Put put : puts) {
			try {
				checkPut(put);
				putList.add(put);
			} catch (AdbClientWithDetailsException e) {
				if (detailException == null) {
					detailException = e;
				} else {
					detailException.merge(e);
				}
			}
		}
		for (Put put : putList) {
			collector.append(put.getRecord());
		}
		if (!asyncCommit) {
			collector.flush(false);
		}
		if (detailException != null) {
			throw detailException;
		}
	}

	public void flush() throws AdbClientException {
		ensurePoolOpen();
		collector.flush(false);
	}

	public boolean isAsyncCommit() {
		return asyncCommit;
	}

	public void setAsyncCommit(boolean asyncCommit) {
		this.asyncCommit = asyncCommit;
	}

	private void closeInternal() {

		if (pool != null && pool.isRegister(this)) {
			try {
				tryThrowException();
				flush();
			} catch (AdbClientException e) {
				LOGGER.error("fail when close", e);
			}
			pool.unregister(this);
			if (isEmbeddedPool) {
				pool.close();
			}
		}
		if (fixedPool != null && fixedPool.isRegister(this)) {
			fixedPool.unregister(this);
			if (isEmbeddedFixedPool) {
				fixedPool.close();
			}
		}
	}

	@Override
	public void close() {
		closeInternal();
	}
}
