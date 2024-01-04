/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.cloud.analyticdb.adb3client.impl;

import com.alibaba.cloud.analyticdb.adb3client.AdbClient;
import com.alibaba.cloud.analyticdb.adb3client.AdbConfig;
import com.alibaba.cloud.analyticdb.adb3client.exception.AdbClientException;
import com.alibaba.cloud.analyticdb.adb3client.exception.ExceptionCode;
import com.alibaba.cloud.analyticdb.adb3client.impl.action.AbstractAction;
import com.alibaba.cloud.analyticdb.adb3client.impl.action.MetaAction;
import com.alibaba.cloud.analyticdb.adb3client.impl.action.PutAction;
import com.alibaba.cloud.analyticdb.adb3client.impl.collector.ActionCollector;
import com.alibaba.cloud.analyticdb.adb3client.model.TableName;
import com.alibaba.cloud.analyticdb.adb3client.model.TableSchema;
import com.alibaba.cloud.analyticdb.adb3client.util.Tuple;
import com.alibaba.druid.pool.DruidDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * 执行资源池，维护请求和工作线程.
 * 请求： clientMap，所有AdbCLient和ActionColllector的映射
 * 工作线程：
 * 1 workers，每个worker维护一个jdbc连接，处理Action
 * 2 commitTimer(commitJob), 定时调用所有collector的tryCommit方法
 * 3 readActionWatcher， 监控读请求队列，来了就第一时间丢给worker
 */
public class ExecutionPool implements Closeable {

	public static final Logger LOGGER = LoggerFactory.getLogger(ExecutionPool.class);

	static final Map<String, ExecutionPool> POOL_MAP = new ConcurrentHashMap<>();

	//----------------------各类工作线程--------------------------------------
	/**
	 * 后台线程.
	 * 1 触发所有的collector去检查是否满足writeMaxIntervalMs条件，及时像后端提交任务
	 * 2 尝试刷新满足metaAutoRefreshFactor的表的tableSchema
	 */
	private Runnable backgroundJob;
	//处理所有Action的runnable，push模式，由其他线程主动把任务塞給worker
	private Worker[] workers;
	private Semaphore writeSemaphore;
	private Semaphore readSemaphore;

	//挂到shutdownHook上，免得用户忘记关闭了
	Thread shutdownHandler = null;

	//----------------------------------------------------------------------

	private String name;
	private Map<AdbClient, ActionCollector> clientMap;

	private AtomicBoolean started; //executionPool整体是否在运行中 ，false以后submit将抛异常
	private AtomicBoolean workerStated; //worker是否在运行中，false以后worker.offer将抛异常

	private Tuple<String, AdbClientException> fatalException = null;
	final ByteSizeCache byteSizeCache;

	private final Cache<TableName, TableSchema> tableCache;

	ExecutorService workerExecutorService;
	ThreadFactory workerThreadFactory;

	ExecutorService backgroundExecutorService;
	ThreadFactory backgroundThreadFactory;

	ThreadFactory ontShotWorkerThreadFactory;
	DruidDataSource dataSource;

	final int writeThreadSize;
	final boolean enableShutdownHook;
	final AdbConfig config;
	final boolean isFixedPool; //当前ExecutionPool是不是fixed fe execution Pool

	public static ExecutionPool buildOrGet(String name, AdbConfig config) {
		return buildOrGet(name, config, false);
	}

	public static ExecutionPool buildOrGet(String name, AdbConfig config, boolean isFixedPool) {
		synchronized (POOL_MAP) {
			return POOL_MAP.computeIfAbsent(name, n -> new ExecutionPool(n, config, isFixedPool));
		}
	}

	public static ExecutionPool getInstance(String name) {
		return POOL_MAP.get(name);
	}

	public ExecutionPool(String name, AdbConfig config, boolean isFixedPool) {
		this.name = name;
		this.config = config;
		this.isFixedPool = isFixedPool;
		workerThreadFactory = new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r);
				t.setName(ExecutionPool.this.name + "-worker");
				t.setDaemon(false);
				return t;
			}
		};
		backgroundThreadFactory = new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r);
				t.setName(ExecutionPool.this.name + "-background");
				t.setDaemon(false);
				return t;
			}
		};
		ontShotWorkerThreadFactory = new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r);
				t.setName(ExecutionPool.this.name + "-oneshot-worker");
				t.setDaemon(false);
				return t;
			}
		};
		this.writeThreadSize = config.getWriteThreadSize();
		this.enableShutdownHook = config.isEnableShutdownHook();
		// Fe模式: workerSize取读并发和写并发的最大值，worker会公用
		// FixedFe模式: 分为fixedPool（workerSize取读并发和写并发的最大值，worker会公用）和fePool（workerSize设置为connectionSizeWhenUseFixedFe, 用于sql、meta等其他action）
		int workerSize;
		if (config.isUseFixedFe() && !isFixedPool) {
			workerSize = config.getConnectionSizeWhenUseFixedFe();
		} else {
			workerSize = writeThreadSize;
		}
		workers = new Worker[workerSize];
		started = new AtomicBoolean(false);
		workerStated = new AtomicBoolean(false);
		dataSource = new DruidDataSource();
		// 配置连接池相关参数
		dataSource.setUrl(addRewriteBatchedStatements(config.getJdbcUrl()));
		dataSource.setUsername(config.getUsername());
		dataSource.setPassword(config.getPassword());
		dataSource.setDriverClassName("com.mysql.jdbc.Driver");

		dataSource.setInitialSize(workerSize);
		dataSource.setMinIdle(workerSize);
		dataSource.setMaxActive(workerSize * 2);
		dataSource.setMaxWait(60000);
		dataSource.setMinEvictableIdleTimeMillis(600000);
		dataSource.setMaxEvictableIdleTimeMillis(900000);
		dataSource.setTimeBetweenEvictionRunsMillis(2000);
		dataSource.setTestWhileIdle(true);
		dataSource.setTestOnBorrow(false);
		dataSource.setTestOnReturn(false);
		dataSource.setKeepAlive(true);
		dataSource.setKeepAliveBetweenTimeMillis(30000);
		dataSource.setPhyMaxUseCount(1000);
		dataSource.setValidationQuery("select 1");
		for (int i = 0; i < workerSize; ++i) {
			if (isFixedPool) {
				workers[i] = new Worker(config, dataSource, workerStated, i, true);
			} else {
				workers[i] = new Worker(config, dataSource, workerStated, i);
			}
		}

		clientMap = new ConcurrentHashMap<>();
		byteSizeCache = new ByteSizeCache(config.getWriteBatchTotalByteSize());
		backgroundJob = new BackgroundJob(config);
		this.tableCache = new Cache<>(config.getMetaCacheTTL(), null);
	}

	private String addRewriteBatchedStatements(final String jdbcUrl) {
		// 检查URL是否已经包含参数
		if (jdbcUrl.contains("rewriteBatchedStatements=")) {
			return jdbcUrl;
		}

		// 判断URL是否已经包含其他参数
		if (jdbcUrl.contains("?")) {
			// 如果URL中已有参数，使用'&'来添加新参数
			return jdbcUrl + "&rewriteBatchedStatements=true";
		} else {
			// 如果URL中没有参数，使用'?'来添加参数
			return jdbcUrl + "?rewriteBatchedStatements=true";
		}
	}

	private synchronized void start() throws AdbClientException {
		if (started.compareAndSet(false, true)) {
			LOGGER.info("AdbClient ExecutionPool[{}] start", name);
			closeStack = null;
			workerStated.set(true);
			workerExecutorService = new ThreadPoolExecutor(workers.length, workers.length, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1), workerThreadFactory, new ThreadPoolExecutor.AbortPolicy());
			backgroundExecutorService = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1), backgroundThreadFactory, new ThreadPoolExecutor.AbortPolicy());
			for (int i = 0; i < workers.length; ++i) {
				workerExecutorService.execute(workers[i]);
			}
			if (this.enableShutdownHook) {
				shutdownHandler = new Thread(() -> close());
				Runtime.getRuntime().addShutdownHook(shutdownHandler);
			}
			backgroundExecutorService.execute(backgroundJob);
			this.writeSemaphore = new Semaphore(this.writeThreadSize);
		}
	}

	Tuple<String, AdbClientException> closeStack = null;

	public synchronized Tuple<String, AdbClientException> getCloseReasonStack() {
		return closeStack;
	}

	public boolean isFixedPool() {
		return isFixedPool;
	}

	@Override
	public synchronized void close() {
		if (started.compareAndSet(true, false)) {
			closeStack = new Tuple<>(LocalDateTime.now().toString(),
				new AdbClientException(ExceptionCode.ALREADY_CLOSE, "close caused by"));
			if (clientMap.size() > 0) {
				LOGGER.warn("AdbClient ExecutionPool[{}] close, current client size {}", name, clientMap.size());
			} else {
				LOGGER.info("AdbClient ExecutionPool[{}] close", name);
			}
			if (shutdownHandler != null) {
				try {
					Runtime.getRuntime().removeShutdownHook(shutdownHandler);
				} catch (Exception e) {
					LOGGER.warn("", e);
				}
			}
			try {
				backgroundExecutorService.shutdownNow();
				while (!backgroundExecutorService.awaitTermination(500L, TimeUnit.MILLISECONDS)) {
					LOGGER.info("wait background executorService termination[{}]", name);
				}
				backgroundExecutorService = null;
			} catch (InterruptedException ignore) {
			}
			workerStated.set(false);
			try {
				workerExecutorService.shutdown();
				while (!workerExecutorService.awaitTermination(500L, TimeUnit.MILLISECONDS)) {
					LOGGER.info("wait worker executorService termination[{}]", name);
				}
				workerExecutorService = null;
				backgroundExecutorService = null;
			} catch (InterruptedException ignore) {
			}
			this.writeSemaphore = null;
			this.readSemaphore = null;
			this.dataSource.close();

			synchronized (POOL_MAP) {
				POOL_MAP.remove(name);
			}
		}
	}

	public TableSchema getOrSubmitTableSchema(TableName tableName, boolean noCache) throws AdbClientException {

		try {
			return tableCache.get(tableName, (tn) -> {
				try {
					MetaAction metaAction = new MetaAction(tableName);
					while (!submit(metaAction)) {
					}
					return metaAction.getResult();
				} catch (AdbClientException e) {
					throw new SQLException(e);
				}
			}, noCache ? Cache.MODE_NO_CACHE : Cache.MODE_LOCAL_THEN_REMOTE);
		} catch (SQLException e) {
			throw AdbClientException.fromSqlException(e);
		}
	}


	/**
	 * oneshot是靠started自己去控制的，如果started一直不false，也就不会结束.
	 *
	 * @param //started
	 * @param //index
	 * @param action
	 * @return
	 * @throws AdbClientException
	 *
	public Thread submitOneShotAction(AtomicBoolean started, int index, AbstractAction action) throws AdbClientException {
		Worker worker = new Worker(config, started, index, isShardEnv);
		boolean ret = worker.offer(action);
		if (!ret) {
			throw new AdbClientException(ExceptionCode.INTERNAL_ERROR, "submitOneShotAction fail");
		}
		Thread thread = ontShotWorkerThreadFactory.newThread(worker);
		thread.start();
		return thread;
	}

	/**
	 * @param action action
	 * @return 提交成功返回true；所有worker都忙时返回false
	 */
	public boolean submit(AbstractAction action) throws AdbClientException {
		if (!started.get()) {
			throw new AdbClientException(ExceptionCode.ALREADY_CLOSE, "submit fail");
		}
		Semaphore semaphore = null;
		int start = -1;
		int end = -1;
		if (action instanceof PutAction) {
			semaphore = writeSemaphore;
			start = 0;
			end = Math.min(writeThreadSize, workers.length);
		} else {
			start = 0;
			end = workers.length;
		}

		//如果有信号量，尝试获取信号量，否则返回submit失败
		if (semaphore != null) {
			try {
				boolean acquire = semaphore.tryAcquire(2000L, TimeUnit.MILLISECONDS);
				if (!acquire) {
					return false;
				}
			} catch (InterruptedException e) {
				throw new AdbClientException(ExceptionCode.INTERRUPTED, "");
			}
			action.setSemaphore(semaphore);
		}
		//尝试提交
		for (int i = start; i < end; ++i) {
			Worker worker = workers[i];
			if (worker.offer(action)) {
				return true;
			}
		}
		//提交失败则释放,提交成功Worker会负责释放
		if (semaphore != null) {
			semaphore.release();
		}
		return false;
	}

	/*public MetaStore getMetaStore() {
		return metaStore;
	}

	public int getWorkerCount() {
		return workers.length;
	}*/

	public ActionCollector register(AdbClient client, AdbConfig config) throws AdbClientException {
		boolean needStart = false;
		ActionCollector collector = null;
		synchronized (clientMap) {
			boolean empty = clientMap.isEmpty();
			collector = clientMap.get(client);
			if (collector == null) {
				LOGGER.info("register client {}, client size {}->{}", client, clientMap.size(), clientMap.size() + 1);
				collector = new ActionCollector(config, this);
				clientMap.put(client, collector);
				if (empty) {
					needStart = true;
				}
			}
			if (needStart) {
				start();
			}
		}

		return collector;
	}

	public synchronized boolean isRegister(AdbClient client) {
		synchronized (clientMap) {
			return clientMap.containsKey(client);
		}
	}

	public synchronized void unregister(AdbClient client) {
		boolean needClose = false;
		synchronized (clientMap) {
			int oldSize = clientMap.size();
			if (oldSize > 0) {
				clientMap.remove(client);
				int newSize = clientMap.size();
				LOGGER.info("unregister client {}, client size {}->{}", client, oldSize, newSize);
				if (newSize == 0) {
					needClose = true;
				}
			}
		}
		if (needClose) {
			close();
		}
	}

	public boolean isRunning() {
		return started.get();
	}

	public void tryThrowException() throws AdbClientException {
		if (fatalException != null) {
			throw new AdbClientException(fatalException.r.getCode(), String.format(
				"An exception occurred at %s and data may be lost, please restart adb-client and recover from the "
					+ "last checkpoint",
				fatalException.l), fatalException.r);
		}
	}

	/**
	 * 整个ExecutionPool的内存估算.
	 */
	class ByteSizeCache {
		final long maxByteSize;
		long value = 0L;
		AtomicLong last = new AtomicLong(System.nanoTime());

		public ByteSizeCache(long maxByteSize) {
			this.maxByteSize = maxByteSize;
		}

		long getAvailableByteSize() {
			return maxByteSize - getByteSize();
		}

		long getByteSize() {
			long nano = last.get();
			long current = System.nanoTime();
			if (current - nano > 2 * 1000 * 1000000L && last.compareAndSet(nano, current)) {
				long sum = clientMap.values().stream().collect(Collectors.summingLong(ActionCollector::getByteSize));
				value = sum;
			}
			return value;
		}
	}

	public long getAvailableByteSize() {
		return byteSizeCache.getAvailableByteSize();
	}

	class BackgroundJob implements Runnable {

		long tableSchemaRemainLife;
		long forceFlushInterval;
		AtomicInteger pendingRefreshTableSchemaActionCount;

		public BackgroundJob(AdbConfig config) {
			forceFlushInterval = config.getForceFlushInterval();
			tableSchemaRemainLife = config.getMetaCacheTTL() / config.getMetaAutoRefreshFactor();
			pendingRefreshTableSchemaActionCount = new AtomicInteger(0);
		}

		long lastForceFlushMs = -1L;

		private void triggerTryFlush() {
			synchronized (clientMap) {
				boolean force = false;

				if (forceFlushInterval > 0L) {
					long current = System.currentTimeMillis();
					if (current - lastForceFlushMs > forceFlushInterval) {
						force = true;
						lastForceFlushMs = current;
					}
				}
				for (ActionCollector collector : clientMap.values()) {
					try {
						if (force) {
							collector.flush(true);
						} else {
							collector.tryFlush();
						}
					} catch (AdbClientException e) {
						fatalException = new Tuple<>(LocalDateTime.now().toString(), e);
						break;
					}
				}
			}
		}

		private void refreshTableSchema() {
			//避免getTableSchema返回太慢的时候，同个表重复刷新TableSchema
			if (pendingRefreshTableSchemaActionCount.get() == 0) {
				// 万一出现非预期的异常也不会导致线程结束工作
				try {
					tableCache.filterKeys(tableSchemaRemainLife).forEach(tableNameWithState -> {
						Cache.ItemState state = tableNameWithState.l;
						TableName tableName = tableNameWithState.r;
						switch (state) {
							case EXPIRE:
								LOGGER.info("remove expire tableSchema for {}", tableName);
								tableCache.remove(tableName);
								break;
							case NEED_REFRESH:
								try {
									getOrSubmitTableSchema(tableName, true);
									MetaAction metaAction = new MetaAction(tableName);
									if (submit(metaAction)) {
										LOGGER.info("refresh tableSchema for {}, because remain lifetime < {} ms", tableName, tableSchemaRemainLife);
										pendingRefreshTableSchemaActionCount.incrementAndGet();
										metaAction.getFuture().whenCompleteAsync((tableSchema, exception) -> {
											pendingRefreshTableSchemaActionCount.decrementAndGet();
											if (exception != null) {
												LOGGER.warn("refreshTableSchema fail", exception);
												if (exception.getMessage() != null && exception.getMessage().contains("can not found table")) {
													tableCache.remove(tableName);
												}
											} else {
												tableCache.put(tableName, tableSchema);
											}
										});
									}
								} catch (Exception e) {
									LOGGER.warn("refreshTableSchema fail", e);
								}
								break;
							default:
								LOGGER.error("undefine item state {}", state);
						}

					});
				} catch (Throwable e) {
					LOGGER.warn("refreshTableSchema unexpected fail", e);
				}
			}
		}

		@Override
		public void run() {
			while (started.get()) {
				triggerTryFlush();
				refreshTableSchema();
				try {
					Thread.sleep(1000L);
				} catch (InterruptedException ignore) {
					break;
				}
			}
		}

		@Override
		public String toString() {
			return "CommitJob";
		}
	}
}
