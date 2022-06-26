package org.mengyun.tcctransaction.repository.helper;
// redis 命令 callback
public interface CommandCallback<T> {
    T execute(RedisCommands commands);
}
