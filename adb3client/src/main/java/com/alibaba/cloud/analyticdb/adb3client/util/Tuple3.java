/*
 * Copyright (c) 2022. Alibaba Group Holding Limited
 */

package com.alibaba.cloud.analyticdb.adb3client.util;

import java.util.Objects;

/**
 * 元组.
 *
 * @param <L> 元素
 * @param <M> 元素
 * @param <R> 元素
 */
public class Tuple3<L, M, R> {
    public L l;
    public M m;
    public R r;

    public Tuple3(L l, M m, R r) {
        this.l = l;
        this.m = m;
        this.r = r;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Tuple3<?, ?, ?> tuple = (Tuple3<?, ?, ?>) o;
        return Objects.equals(l, tuple.l) &&
            Objects.equals(m, tuple.m) &&
            Objects.equals(r, tuple.r);
    }

    @Override
    public int hashCode() {
        return Objects.hash(l, m, r);
    }
}
