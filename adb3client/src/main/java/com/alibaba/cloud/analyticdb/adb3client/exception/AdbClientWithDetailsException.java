/*
 * Copyright (c) 2020. Alibaba Group Holding Limited
 */

package com.alibaba.cloud.analyticdb.adb3client.exception;

import com.alibaba.cloud.analyticdb.adb3client.model.Record;

import java.util.ArrayList;
import java.util.List;

/**
 * A subclass of {@link AdbClientException}.
 * It is thrown when we have more information about which records were causing which
 * exceptions.
 */
public class AdbClientWithDetailsException extends AdbClientException {

	List<Record> failedList;
	List<AdbClientException> exceptionList;

	public AdbClientWithDetailsException(AdbClientException e) {
		super(e.getCode(), e.getMessage(), e.getCause());
	}

	public AdbClientWithDetailsException(ExceptionCode code, String msg, Record record) {
		super(code, msg);
		AdbClientException e = new AdbClientException(code, msg);
		add(record, e);
	}

	public void add(List<Record> failList, AdbClientException e) {
		for (Record record : failList) {
			add(record, e);
		}
	}

	public void add(Record record, AdbClientException e) {
		if (failedList == null) {
			failedList = new ArrayList<>();
			exceptionList = new ArrayList<>();
		}
		failedList.add(record);
		exceptionList.add(e);
	}

	public int size() {
		return failedList == null ? 0 : failedList.size();
	}

	public Record getFailRecord(int index) {
		return failedList.get(index);
	}

	public AdbClientException getException(int index) {

		return exceptionList.get(index);

	}

	public AdbClientException getException() {
		return exceptionList.get(0);
	}

	public AdbClientWithDetailsException merge(AdbClientWithDetailsException other) {
		this.failedList.addAll(other.failedList);
		this.exceptionList.addAll(other.exceptionList);
		return this;
	}

	@Override
	public String getLocalizedMessage() {
		return getMessage();
	}

	@Override
	public String getMessage() {
		if (size() > 0) {
			StringBuilder sb = new StringBuilder();
			sb.append("failed records " + size() + ", first:" + getFailRecord(0) + ",first err:" + getException(0).getMessage());
			return sb.toString();
		} else {
			return super.getMessage();
		}
	}
}
