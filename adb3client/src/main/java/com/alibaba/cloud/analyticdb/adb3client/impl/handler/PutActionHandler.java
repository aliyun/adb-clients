/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.cloud.analyticdb.adb3client.impl.handler;

import com.alibaba.cloud.analyticdb.adb3client.AdbConfig;
import com.alibaba.cloud.analyticdb.adb3client.exception.AdbClientException;
import com.alibaba.cloud.analyticdb.adb3client.exception.AdbClientWithDetailsException;
import com.alibaba.cloud.analyticdb.adb3client.exception.ExceptionCode;
import com.alibaba.cloud.analyticdb.adb3client.impl.PreparedStatementWithBatchInfo;
import com.alibaba.cloud.analyticdb.adb3client.impl.RetryableDbExecutor;
import com.alibaba.cloud.analyticdb.adb3client.impl.UpsertStatementBuilder;
import com.alibaba.cloud.analyticdb.adb3client.impl.action.PutAction;
import com.alibaba.cloud.analyticdb.adb3client.model.Record;
import com.alibaba.cloud.analyticdb.adb3client.model.WriteFailStrategy;
import com.alibaba.cloud.analyticdb.adb3client.model.WriteMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * PutAction处理类.
 */
public class PutActionHandler extends ActionHandler<PutAction> {

	public static final Logger LOGGER = LoggerFactory.getLogger(PutActionHandler.class);

	private static final String NAME = "put";

	private final AdbConfig config;
	private RetryableDbExecutor dbExecutor;
	private final UpsertStatementBuilder builder;

	public PutActionHandler(RetryableDbExecutor dbExecutor, AdbConfig config) {
		super(config);
		this.config = config;
		this.dbExecutor = dbExecutor;
		this.builder = new UpsertStatementBuilder(config);
	}

	private void markRecordPutSuccess(Record record) {
		if (record.getPutFutures() != null) {
			for (CompletableFuture<Void> future : record.getPutFutures()) {
				try {
					future.complete(null);
				} catch (Exception e) {
					LOGGER.error("markRecordPutSuccess", e);
				}
			}
		}
	}

	private void markRecordPutFail(Record record, AdbClientException e) {
		if (record.getPutFutures() != null) {
			for (CompletableFuture<Void> future : record.getPutFutures()) {
				try {
					future.completeExceptionally(e);
				} catch (Exception e1) {
					LOGGER.error("markRecordPutFail", e1);
				}
			}
		}
	}

	private boolean isDirtyDataException(AdbClientException e) {
		boolean ret = false;
		switch (e.getCode()) {
			case TABLE_NOT_FOUND:
			case CONSTRAINT_VIOLATION:
			case DATA_TYPE_ERROR:
			case DATA_VALUE_ERROR:
				ret = true;
				break;
			default:
		}
		return ret;
	}

	@Override
	public void handle(PutAction action) {
		final List<Record> recordList = action.getRecordList();
		WriteMode mode = action.getWriteMode();
		AdbClientException exception = null;
		try {
			doHandlePutAction(recordList, mode);
			for (Record record : recordList) {
				markRecordPutSuccess(record);
			}
		} catch (AdbClientException e) {
			WriteFailStrategy strategy = config.getWriteFailStrategy();
			if (!isDirtyDataException(e)) {
				exception = e;
				//如果不是脏数据类异常的话，就不要one by one了
				strategy = WriteFailStrategy.NONE;
			}
			boolean useDefaultStrategy = true;
			switch (strategy) {
				case TRY_ONE_BY_ONE:
					LOGGER.warn("write data fail, current WriteFailStrategy is TRY_ONE_BY_ONE", e);
					if (e.getCode() != ExceptionCode.TABLE_NOT_FOUND) {
						List<Record> single = new ArrayList<>(1);
						AdbClientWithDetailsException fails = new AdbClientWithDetailsException(e);
						for (Record record : recordList) {
							try {
								single.add(record);
								doHandlePutAction(single, mode);
								markRecordPutSuccess(record);
							} catch (AdbClientException subE) {
								if (!isDirtyDataException(subE)) {
									exception = subE;
								} else {
									fails.add(record, subE);
								}
								markRecordPutFail(record, subE);
							} catch (Exception subE) {
								//如果是致命错误最后就抛这种类型的错
								exception = new AdbClientException(ExceptionCode.INTERNAL_ERROR, "", subE);
								markRecordPutFail(record, exception);
							} finally {
								single.clear();
							}
						}
						if (exception == null && fails.size() > 0) {
							exception = fails;
						}
						useDefaultStrategy = false;
					}
					break;
				default:
			}
			if (useDefaultStrategy) {
				for (Record record : recordList) {
					markRecordPutFail(record, e);
				}

				if (exception == null) {
					AdbClientWithDetailsException localPutException = new AdbClientWithDetailsException(e);
					localPutException.add(recordList, e);
					exception = localPutException;
				}
			}
		} catch (Exception e) {
			exception = new AdbClientException(ExceptionCode.INTERNAL_ERROR, "", e);
			for (Record record : recordList) {
				markRecordPutFail(record, exception);
			}
		}
		if (exception != null) {
			action.getFuture().completeExceptionally(exception);
		} else {
			action.getFuture().complete(null);
		}
	}

	protected void doHandlePutAction(List<Record> list, WriteMode mode) throws AdbClientException {
		dbExecutor.executeWithRetry(conn -> {
			List<PreparedStatementWithBatchInfo> psArray = builder.buildStatements(conn, list.get(0).getSchema(), list.get(0).getTableName(), list, mode);
			try {

				long startTime = System.nanoTime() / 1000000L;
				long bytes = 0L;
				long batchCount = 0;
				for (PreparedStatementWithBatchInfo ps : psArray) {
					if (ps != null) {
						if (ps.r) {
							ps.l.executeBatch();
						} else {
							ps.l.execute();
						}
					}
					bytes += ps.getByteSize();
					batchCount += ps.getBatchCount();
				}
			} finally {
				for (PreparedStatementWithBatchInfo ps : psArray) {
					if (ps != null && ps.l != null) {
						ps.l.close();
					}
				}
			}
			return null;
		});
	}
}
