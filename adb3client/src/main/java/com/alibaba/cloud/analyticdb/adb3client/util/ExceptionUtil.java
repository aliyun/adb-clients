/*
 * Copyright (c) 2023. Alibaba Group Holding Limited
 */

package com.alibaba.cloud.analyticdb.adb3client.util;

import com.alibaba.cloud.analyticdb.adb3client.exception.AdbClientException;
import com.alibaba.cloud.analyticdb.adb3client.exception.AdbClientWithDetailsException;

/**
 * 异常相关工具类.
 */
public class ExceptionUtil {
	/**
	 * 当a和b同为AdbClientWithDetailsException时，将b合并入a对象，否则返回任何一个异常.
	 *
	 * @param a
	 * @param b
	 * @return
	 */
	public static AdbClientException merge(AdbClientException a, AdbClientException b) {
		if (a == null) {
			return b;
		} else if (b == null) {
			return a;
		} else if (a instanceof AdbClientWithDetailsException) {
			if (b instanceof AdbClientWithDetailsException) {
				((AdbClientWithDetailsException) a).merge((AdbClientWithDetailsException) b);
				return a;
			} else {
				return b;
			}
		} else {
			return a;
		}
	}
}
