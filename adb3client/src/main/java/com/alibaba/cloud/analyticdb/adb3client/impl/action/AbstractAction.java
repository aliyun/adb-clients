/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.cloud.analyticdb.adb3client.impl.action;

import com.alibaba.cloud.analyticdb.adb3client.exception.AdbClientException;
import com.alibaba.cloud.analyticdb.adb3client.exception.ExceptionCode;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;

/**
 * ca.
 *
 * @param <T> t
 */
public abstract class AbstractAction<T> {
	CompletableFuture<T> future;

	Semaphore semaphore;

	public AbstractAction() {
		this.future = new CompletableFuture<>();
	}

	public CompletableFuture<T> getFuture() {
		return future;
	}

	public T getResult() throws AdbClientException {
		try {
			return future.get();
		} catch (InterruptedException e) {
			throw new AdbClientException(ExceptionCode.INTERNAL_ERROR, "interrupt", e);
		} catch (ExecutionException e) {
			Throwable cause = e.getCause();
			if (cause instanceof AdbClientException) {
				throw (AdbClientException) cause;
			} else {
				throw new AdbClientException(ExceptionCode.INTERNAL_ERROR, "", cause);
			}
		}
	}

	public Semaphore getSemaphore() {
		return semaphore;
	}

	public void setSemaphore(Semaphore semaphore) {
		this.semaphore = semaphore;
	}
}
