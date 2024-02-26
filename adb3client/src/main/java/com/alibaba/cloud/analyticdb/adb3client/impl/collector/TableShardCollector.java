/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.cloud.analyticdb.adb3client.impl.collector;

import com.alibaba.cloud.analyticdb.adb3client.AdbConfig;
import com.alibaba.cloud.analyticdb.adb3client.exception.AdbClientException;
import com.alibaba.cloud.analyticdb.adb3client.exception.AdbClientWithDetailsException;
import com.alibaba.cloud.analyticdb.adb3client.exception.ExceptionCode;
import com.alibaba.cloud.analyticdb.adb3client.impl.ExecutionPool;
import com.alibaba.cloud.analyticdb.adb3client.impl.action.PutAction;
import com.alibaba.cloud.analyticdb.adb3client.model.Record;
import com.alibaba.cloud.analyticdb.adb3client.model.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * PutAction收集器（shard级别）.
 * 每一个AdbClient对应一个ActionCollector
 * ActionCollector
 * - TableCollector PutAction收集器（表级别）
 * - TableShardCollector PutAction收集器（shard级别，此shard和ADB的shard是2个概念，仅代表客户端侧的数据攒批的分区）
 */
public class TableShardCollector {
	public static final Logger LOGGER = LoggerFactory.getLogger(TableShardCollector.class);

	private RecordCollector buffer;

	/**
	 * 当前buffer中的TableSchema.
	 */
	private TableSchema currentTableSchema;
	private PutAction activeAction;
	private long activeActionByteSize = 0L;
	private final ExecutionPool pool;

	public TableShardCollector(AdbConfig config, ExecutionPool pool, int size) {
		buffer = new RecordCollector(config, pool, size);
		activeAction = null;
		this.pool = pool;
	}

	public synchronized void append(Record record) throws AdbClientException {
		AdbClientException exception = null;
		if (currentTableSchema == null) {
			currentTableSchema = record.getSchema();
		} else if (!currentTableSchema.equals(record.getSchema())) {
			try {
				flush(true, false, null);
			} catch (AdbClientException e) {
				exception = e;
			}
		}
		boolean full = buffer.append(record);
		if (full) {
			try {
				waitActionDone();
			} catch (AdbClientWithDetailsException e) {
				if (exception == null) {
					exception = e;
				} else if (exception instanceof AdbClientWithDetailsException) {
					((AdbClientWithDetailsException) exception).merge(e);
				}
			} catch (AdbClientException e) {
				exception = e;
			}
			commit(buffer.getBatchState());

		} else {
			try {
				isActionDone();
			} catch (AdbClientWithDetailsException e) {
				if (exception == null) {
					exception = e;
				} else if (exception instanceof AdbClientWithDetailsException) {
					((AdbClientWithDetailsException) exception).merge(e);
				}
			} catch (AdbClientException e) {
				exception = e;
			}
		}
		if (exception != null) {
			throw exception;
		}
	}

	private void commit(BatchState state) throws AdbClientException {
		LOGGER.debug("commit {} rows, {} bytes, state {} ", buffer.getRecords().size(), buffer.getByteSize(), state);
		//System.out.println(String.format("commit %d rows, %d bytes, state %s ", buffer.getRecords().size(), buffer.getByteSize(), state));
		activeAction = new PutAction(buffer.getRecords(), buffer.getByteSize(), buffer.getMode(), state);
		try {
			while (!pool.submit(activeAction)) {
			}
			activeActionByteSize = activeAction.getByteSize();
		} catch (Exception e) {
			activeAction.getFuture().completeExceptionally(e);
			if (activeAction.getRecordList() != null) {
				for (Record record : activeAction.getRecordList()) {
					if (record.getPutFutures() != null) {
						for (CompletableFuture<Void> future : record.getPutFutures()) {
							if (!future.isDone()) {
								future.completeExceptionally(e);
							}
						}
					}
				}
			}
			if (!(e instanceof AdbClientException)) {
				throw new AdbClientException(ExceptionCode.INTERNAL_ERROR, "", e);
			} else {
				throw e;
			}
		} finally {
			buffer.clear();
			// currentTableSchema = tableSchema in buffer.
			currentTableSchema = null;
		}

	}

	private void clearActiveAction() {
		activeAction = null;
		activeActionByteSize = 0L;
	}

	private void waitActionDone() throws AdbClientException {
		if (activeAction != null) {
			try {
				activeAction.getFuture().get();
			} catch (ExecutionException e) {
				Throwable cause = e.getCause();
				if (cause instanceof AdbClientException) {
					throw (AdbClientException) cause;
				} else {
					throw new AdbClientException(ExceptionCode.INTERNAL_ERROR, "unknow exception", cause);
				}
			} catch (InterruptedException e) {
			} finally {
				clearActiveAction();
			}
		}
	}

	private boolean isActionDone() throws AdbClientException {
		if (activeAction != null) {
			try {
				if (activeAction.getFuture().isDone()) {
					//LOGGER.info("Pair Done:{}",readable.getPutAction().getFuture());
					try {
						activeAction.getFuture().get();
					} finally {
						clearActiveAction();
					}
					return true;
				} else {
					return false;
				}
			} catch (ExecutionException e) {
				Throwable cause = e.getCause();
				clearActiveAction();
				if (cause instanceof AdbClientException) {
					throw (AdbClientException) cause;
				} else {
					throw new AdbClientException(ExceptionCode.INTERNAL_ERROR, "unknow exception", cause);
				}
			} catch (InterruptedException e) {
				return false;
			}
		} else {
			return true;
		}
	}

	/**
	 * 是否flush完成.
	 *
	 * @param force                  是否强制flush，强制flush只要buffer.size &gt; 0就一定提交，否则还是看RecordCollector自己判断是不是应该提交
	 * @param async                  是否异步，同步的话，对于activeAction会wait到完成为止
	 * @param uncommittedActionCount 如果activeAction未完成，并且buffer.size &gt; 0 ，加一，表示还有任务没有提交給worker
	 * @return true, 没有任何pending的记录
	 * @throws AdbClientException 异常
	 */
	public synchronized boolean flush(boolean force, boolean async, AtomicInteger uncommittedActionCount) throws AdbClientException {
		AdbClientWithDetailsException failedRecords = null;

		boolean readableDone = false;
		try {
			if (async) {
				readableDone = isActionDone();
			} else {
				readableDone = true;
				waitActionDone();
			}
		} catch (AdbClientWithDetailsException e) {
			readableDone = true;
			failedRecords = e;
		}
		boolean done = false;
		if (readableDone) {
			if (buffer.size > 0) {
				BatchState state = force ? BatchState.Force : buffer.getBatchState();
				if (state != BatchState.NotEnough) {
					commit(state);
				}
			} else {
				done = true;
			}
		} else if (uncommittedActionCount != null) {
			if (buffer.size > 0) {
				uncommittedActionCount.incrementAndGet();
			}
		}

		if (failedRecords != null) {
			throw failedRecords;
		}

		return done;
	}

	public long getByteSize() {
		return activeActionByteSize + buffer.getByteSize();
	}
}
