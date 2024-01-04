package com.alibaba.cloud.analyticdb.adb3client.util;

import java.util.Arrays;
import java.util.List;

/**
 * ArrayUtil.
 */

public class ArrayUtil {
	public static void reverse(byte[] array) {
		if (array != null) {
			int i = 0;

			for (int j = array.length - 1; j > i; ++i) {
				byte tmp = array[j];
				array[j] = array[i];
				array[i] = tmp;
				--j;
			}
		}
	}

	public static long getArrayLength(String[] array) {
		long len = 0;
		if (array != null) {
			for (String str : array) {
				if (str != null) {
					len += str.length();
				}
			}
		}
		return len;
	}

	public static long getArrayLength(Object[] array, String typeName) {
		long len = 0;
		if (array != null) {
			switch (typeName) {
				case "_int4":
				case "_float4":
					len = array.length * 4L;
					break;
				case "_int8":
				case "_float8":
					len = array.length * 8L;
					break;
				case "_bool":
					len = array.length;
					break;
				case "_text":
					for (Object str : array) {
						if (str != null) {
							len += str.toString().length();
						}
					}
					break;
				default:
					len = 32;
			}
		}
		return len;
	}

	public static long getArrayLength(List<?> array, String typeName) {
		long len = 0;
		if (array != null) {
			switch (typeName) {
				case "_int4":
				case "_float4":
					len = array.size() * 4L;
					break;
				case "_int8":
				case "_float8":
					len = array.size() * 8L;
					break;
				case "_bool":
					len = array.size();
					break;
				case "_text":
					for (Object str : array) {
						if (str != null) {
							len += str.toString().length();
						}
					}
					break;
				default:
					len = 32;
			}
		}
		return len;
	}

	public static <T> T[] arrayConcat(T[] a, T[] b) {
		T[] result = Arrays.copyOf(a, a.length + b.length);
		System.arraycopy(b, 0, result, a.length, b.length);
		return result;
	}
}
