package org.mengyun.tcctransaction.repository.helper;

import java.io.Closeable;
import java.util.List;

public interface ShardHolder<T> extends Closeable {
    List<T> getAllShards(); // 获取所有分片
}
