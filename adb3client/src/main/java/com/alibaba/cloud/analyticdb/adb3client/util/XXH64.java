package com.alibaba.cloud.analyticdb.adb3client.util;
/**
 * xxHash64. A high quality and fast 64 bit hash code by Yann Colet and Mathias Westerdahl. The
 * class below is modelled like its Murmur3_x86_32 cousin.
 * <p></p>
 * This was largely based on the following (original) C and Java implementations:
 * https://github.com/Cyan4973/xxHash/blob/master/xxhash.c
 * https://github.com/OpenHFT/Zero-Allocation-Hashing/blob/master/src/main/java/net/openhft/hashing/XxHash_r39.java
 * https://github.com/airlift/slice/blob/master/src/main/java/io/airlift/slice/XxHash64.java
 */
public final class XXH64 {

	private static final long PRIME64_1 = 0x9E3779B185EBCA87L;
	private static final long PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
	private static final long PRIME64_3 = 0x165667B19E3779F9L;
	private static final long PRIME64_4 = 0x85EBCA77C2B2AE63L;
	private static final long PRIME64_5 = 0x27D4EB2F165667C5L;

	public static long hashLong(long input, long seed) {
		long hash = seed + PRIME64_5 + 8L;
		hash ^= Long.rotateLeft(input * PRIME64_2, 31) * PRIME64_1;
		hash = Long.rotateLeft(hash, 27) * PRIME64_1 + PRIME64_4;
		return fmix(hash);
	}

	private static long fmix(long hash) {
		hash ^= hash >>> 33;
		hash *= PRIME64_2;
		hash ^= hash >>> 29;
		hash *= PRIME64_3;
		hash ^= hash >>> 32;
		return hash;
	}

}