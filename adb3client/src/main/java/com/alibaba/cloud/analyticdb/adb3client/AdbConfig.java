/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.cloud.analyticdb.adb3client;

import com.alibaba.cloud.analyticdb.adb3client.model.WriteFailStrategy;
import com.alibaba.cloud.analyticdb.adb3client.model.WriteMode;

import java.io.Serializable;
import java.lang.reflect.Field;

/**
 * config class for adb3 client.
 */
public class AdbConfig implements Serializable {

	public static final int DEFAULT_BATCH_SIZE = 512;
	public static final long DEFAULT_BATCH_BYTE_SIZE = 2L * 1024L * 1024L;
	public static final WriteMode DEFAULT_WRITE_MODE = WriteMode.INSERT_OR_REPLACE;

	//----------------------------write conf--------------------------------------------
	/**
	 * 在AsyncCommit为true，调用put方法时，当记录数 &ge; writeBatchSize 或 总记录字节数数 &ge; writeBatchByteSize.
	 * 调用flush进行批量提交.
	 * 默认为512
	 *
	 * 
	 * 
	 */
	int writeBatchSize = DEFAULT_BATCH_SIZE;

	/**
	 * 在AsyncCommit为true，调用put方法时，当记录数 &ge; writeBatchSize 或 总记录字节数数 &ge; writeBatchByteSize.
	 * 调用flush进行List批量提交.
	 * 默认为2MB
	 *
	 * 
	 * 
	 */
	long writeBatchByteSize = DEFAULT_BATCH_BYTE_SIZE;

	/**
	 * 所有表攒批总共的最大batchSize
	 * 调用flush进行List批量提交.
	 * 默认为20MB
	 *
	 * 
	 * 
	 */
	long writeBatchTotalByteSize = DEFAULT_BATCH_BYTE_SIZE * 10;

	/**
	 * 当INSERT目标表为有主键的表时采用不同策略.
	 * INSERT_OR_IGNORE 当主键冲突时，不写入
	 * INSERT_OR_UPDATE 当主键冲突时，更新相应列
	 * INSERT_OR_REPLACE当主键冲突时，更新所有列
	 *
	 * 
	 * 
	 */
	WriteMode writeMode = DEFAULT_WRITE_MODE;

	/**
	 * 在AsyncCommit为true，当记录数&ge;writeBatchSize 或总记录字节数数 &ge; writeBatchByteSize 或距离上次flush超过writeMaxIntervalMs毫秒 调用flush进行提交     * 默认为100MB.
	 *
	 * 
	 * 
	 */
	long writeMaxIntervalMs = 10000L;

	/**
	 * 当INSERT失败采取的策略.
	 * TRY_ONE_BY_ONE
	 * NONE
	 *
	 * 
	 * 
	 */
	WriteFailStrategy writeFailStrategy = WriteFailStrategy.TRY_ONE_BY_ONE;

	/**
	 * put操作的并发数.
	 *
	 * 
	 * 
	 */
	int writeThreadSize = 1;

	/**
	 * 当将Number写入Date/timestamp/timestamptz列时，将number视作距离1970-01-01 00:00:00 +00:00的毫秒数.
	 *
	 * 
	 * 
	 */
	boolean inputNumberAsEpochMsForDatetimeColumn = false;

	/**
	 * 当将Number写入Date/timestamp/timestamptz列时，将number视作距离1970-01-01 00:00:00 +00:00的毫秒数.
	 *
	 * 
	 * 
	 */
	boolean inputStringAsEpochMsForDatetimeColumn = false;

	/**
	 * 启用时，not null且未在表上设置default的字段传入null时，将转为默认值.
	 * String
	 *
	 * 
	 * 
	 */
	boolean enableDefaultForNotNullColumn = true;

	/**
	 * defaultTimestamp.
	 * String
	 *
	 * 
	 * 
	 */
	String defaultTimestampText = null;

	/**
	 * 开启将text列value中的\u0000替换为"".
	 * boolean
	 *
	 * 
	 * 
	 */
	boolean removeU0000InTextColumnValue = true;

	/**
	 * 全局flush的时间间隔.
	 * boolean
	 *
	 * 
	 * 
	 */
	long forceFlushInterval = -1L;

	/**
	 * 多久打印一次写入数据采样.
	 * int
	 *
	 * 
	 * 
	 */
	long recordSampleInterval = -1L;

	//---------------------------conn conf------------------------------------------
	/**
	 * 请求重试次数，默认3.
	 * 这个名字应该叫做maxTryCount，而不是retryCount，设为1其实是不会retry的
	 *
	 * 
	 * 
	 */
	int retryCount = 3;

	/**
	 * 每次重试等待时间为  当前重试次数*retrySleepMs + retrySleepInitMs.
	 *
	 * 
	 * 
	 */
	long retrySleepStepMs = 10000L;

	/**
	 * 每次重试等待时间为  当前重试次数*retrySleepMs + retrySleepInitMs.
	 *
	 * 
	 * 
	 */
	long retrySleepInitMs = 1000L;

	/**
	 * meta信息缓存时间(ms).
	 *
	 * 
	 * 
	 */
	long metaCacheTTL = 60000L;

	/**
	 * meta缓存剩余时间低于 metaCacheTTL/metaAutoRefreshFactor 将被自动刷新.
	 *
	 * 
	 * 
	 */
	int metaAutoRefreshFactor = 4;

	//------------------------endpoint conf--------------------------------------------
	/**
	 * 顾名思义，jdbcUrl.
	 *
	 * 
	 * 
	 */
	String jdbcUrl;

	/**
	 * jdbcUrl，必填.
	 *
	 * 
	 * 
	 */
	String username;
	/**
	 * jdbcUrl，必填.
	 *
	 * 
	 * 
	 */
	String password;

	/**
	 * 是否使用fixed fe模式进行数据写入和点查.
	 *
	 * 
	 * 
	 */
	boolean useFixedFe = false;

	/**
	 * 使用fixed fe进行数据写入和点查时， 其他action使用的连接数.
	 *
	 * 
	 * 
	 */
	int connectionSizeWhenUseFixedFe = 1;

	String appName = "adb3client";

	boolean enableShutdownHook = false;

	public WriteMode getWriteMode() {
		return writeMode;
	}

	public void setWriteMode(WriteMode writeMode) {
		this.writeMode = writeMode;
	}

	public String getJdbcUrl() {
		return jdbcUrl;
	}

	public void setJdbcUrl(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public boolean isUseFixedFe() {
		return useFixedFe;
	}

	public void setUseFixedFe(boolean useFixedFe) {
		this.useFixedFe = useFixedFe;
	}

	public int getConnectionSizeWhenUseFixedFe() {
		return connectionSizeWhenUseFixedFe;
	}

	public void setConnectionSizeWhenUseFixedFe(int connectionSizeWhenUseFixedFe) {
		this.connectionSizeWhenUseFixedFe = connectionSizeWhenUseFixedFe;
	}

	public int getWriteBatchSize() {
		return writeBatchSize;
	}

	public void setWriteBatchSize(int batchSize) {
		this.writeBatchSize = batchSize;
	}

	public long getWriteBatchByteSize() {
		return writeBatchByteSize;
	}

	public void setWriteBatchByteSize(long batchByteSize) {
		this.writeBatchByteSize = batchByteSize;
	}

	public int getRetryCount() {
		return retryCount;
	}

	public void setRetryCount(int retryCount) {
		this.retryCount = retryCount;
	}

	public long getRetrySleepStepMs() {
		return retrySleepStepMs;
	}

	public void setRetrySleepStepMs(long retrySleepStepMs) {
		this.retrySleepStepMs = retrySleepStepMs;
	}

	public long getRetrySleepInitMs() {
		return retrySleepInitMs;
	}

	public void setRetrySleepInitMs(long retrySleepInitMs) {
		this.retrySleepInitMs = retrySleepInitMs;
	}

	public long getWriteMaxIntervalMs() {
		return writeMaxIntervalMs;
	}

	public void setWriteMaxIntervalMs(long writeMaxIntervalMs) {
		this.writeMaxIntervalMs = writeMaxIntervalMs;
	}

	@Deprecated
	public WriteFailStrategy getWriteFailStrategy() {
		return writeFailStrategy;
	}

	@Deprecated
	public void setWriteFailStrategy(WriteFailStrategy writeFailStrategy) {
		this.writeFailStrategy = writeFailStrategy;
	}

	public int getWriteThreadSize() {
		return writeThreadSize;
	}

	public void setWriteThreadSize(int writeThreadSize) {
		this.writeThreadSize = writeThreadSize;
	}

	public boolean isInputNumberAsEpochMsForDatetimeColumn() {
		return inputNumberAsEpochMsForDatetimeColumn;
	}

	public void setInputNumberAsEpochMsForDatetimeColumn(boolean inputNumberAsEpochMsForDatetimeColumn) {
		this.inputNumberAsEpochMsForDatetimeColumn = inputNumberAsEpochMsForDatetimeColumn;
	}

	public long getMetaCacheTTL() {
		return metaCacheTTL;
	}

	public void setMetaCacheTTL(long metaCacheTTL) {
		this.metaCacheTTL = metaCacheTTL;
	}

	public boolean isInputStringAsEpochMsForDatetimeColumn() {
		return inputStringAsEpochMsForDatetimeColumn;
	}

	public void setInputStringAsEpochMsForDatetimeColumn(boolean inputStringAsEpochMsForDatetimeColumn) {
		this.inputStringAsEpochMsForDatetimeColumn = inputStringAsEpochMsForDatetimeColumn;
	}

	public boolean isEnableDefaultForNotNullColumn() {
		return enableDefaultForNotNullColumn;
	}

	public void setEnableDefaultForNotNullColumn(boolean enableDefaultForNotNullColumn) {
		this.enableDefaultForNotNullColumn = enableDefaultForNotNullColumn;
	}

	public String getDefaultTimestampText() {
		return defaultTimestampText;
	}

	public void setDefaultTimestampText(String defaultTimestampText) {
		this.defaultTimestampText = defaultTimestampText;
	}

	public long getWriteBatchTotalByteSize() {
		return writeBatchTotalByteSize;
	}

	public void setWriteBatchTotalByteSize(long writeBatchTotalByteSize) {
		this.writeBatchTotalByteSize = writeBatchTotalByteSize;
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public int getMetaAutoRefreshFactor() {
		return metaAutoRefreshFactor;
	}

	public void setMetaAutoRefreshFactor(int metaAutoRefreshFactor) {
		this.metaAutoRefreshFactor = metaAutoRefreshFactor;
	}

	public boolean isRemoveU0000InTextColumnValue() {
		return removeU0000InTextColumnValue;
	}

	public void setRemoveU0000InTextColumnValue(boolean removeU0000InTextColumnValue) {
		this.removeU0000InTextColumnValue = removeU0000InTextColumnValue;
	}

	public long getForceFlushInterval() {
		return forceFlushInterval;
	}

	public void setForceFlushInterval(long forceFlushInterval) {
		this.forceFlushInterval = forceFlushInterval;
	}

	public long getRecordSampleInterval() {
		return recordSampleInterval;
	}

	public void setRecordSampleInterval(long recordSampleInterval) {
		this.recordSampleInterval = recordSampleInterval;
	}

	public boolean isEnableShutdownHook() {
		return enableShutdownHook;
	}

	public void setEnableShutdownHook(boolean enableShutdownHook) {
		this.enableShutdownHook = enableShutdownHook;
	}

	public static String[] getPropertyKeys() {
		Field[] fields = AdbConfig.class.getDeclaredFields();
		String[] propertyKeys = new String[fields.length];
		int index = 0;
		for (Field field : fields) {
			propertyKeys[index++] = field.getName();
		}
		return propertyKeys;
	}
}
