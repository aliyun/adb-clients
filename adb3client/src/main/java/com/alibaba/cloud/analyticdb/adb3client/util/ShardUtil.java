/*
 * Copyright (c) 2021. Alibaba Group Holding Limited
 */

package com.alibaba.cloud.analyticdb.adb3client.util;

import com.alibaba.cloud.analyticdb.adb3client.model.Record;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.lang.reflect.Array;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

/**
 * shard相关的工具方法.
 */
public class ShardUtil {

	private static final Charset UTF8 = Charset.forName("UTF-8");
	public static final int NULL_HASH_CODE = 0;

	public static int hash(Record record, int[] indexes) {
		int hash = 0;
		boolean first = true;
		if (indexes == null || indexes.length == 0) {
			ThreadLocalRandom rand = ThreadLocalRandom.current();
			hash = rand.nextInt();
		} else {
			for (int i : indexes) {
				if (first) {
					hash = ShardUtil.hash(record.getObject(i));
				} else {
					hash ^= ShardUtil.hash(record.getObject(i));
				}
				first = false;
			}
		}
		return hash;
	}

	public static long hashLong(Record record, int[] indexes) {
		if (indexes == null || indexes.length == 0) {
			ThreadLocalRandom rand = ThreadLocalRandom.current();
			return rand.nextLong();
		}

		long result = 0;
		for (int i : indexes) {
			result = XXH64.hashLong(ShardUtil.hash(record.getObject(i)), result);
		}
		return Math.abs(result);
	}

	public static int hash(Object obj) {
		if (obj == null) {
			return NULL_HASH_CODE;
		} else {
			if (obj instanceof String) {
				byte[] value = ((String) obj).getBytes(UTF8);
				int result = 1;
				for (int i = 0; i < value.length; i++) {
					result = 31 * result + value[i];
				}
				return result;
			} else if (obj instanceof byte[]) {
				return Arrays.hashCode((byte[]) obj);
			} else if (obj instanceof Boolean) {
				return (Boolean) obj ? 1 : 0;
			} else if (obj instanceof Byte || obj instanceof Integer || obj instanceof Short) {
				return (int) obj;
			} else if (obj instanceof Date) {
				long time = ((Date) obj).getTime();
				int result = (int) (time ^ (time >>> 32));
				return result;
			} else if (obj instanceof Long) {
				long longVal = (Long) obj;
				return (int) (longVal ^ (longVal >>> 32));
			} else if (obj.getClass().isArray()) {
				int hash = 0;
				int length = Array.getLength(obj);
				for (int i = 0; i < length; ++i) {
					Object child = Array.get(obj, i);
					hash = hash * 31 + (child == null ? 0 : hash(child));
				}
				return hash;
			} else {
				return hash(String.valueOf(obj));
			}
		}
	}
}
