/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.cloud.analyticdb.adb3client.util;

/**
 * Util class to quote ADB identifier.
 */
public class IdentifierUtil {
	/**
	 * Quote an identifier (e.g. column name, table name) with ``.
	 *
	 * @param name the identifier to quote
	 * @return the quoted result.
	 */

	public static String quoteIdentifier(String name) {
		return new StringBuilder("`").append(name).append("`").toString();
	}
}
