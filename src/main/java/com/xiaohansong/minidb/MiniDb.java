package com.xiaohansong.minidb;

/**
 * 数据库接口
 */
public interface MiniDb {

    void put(byte[] key, byte[] value);

    byte[] get(byte[] key);

    void delete(byte[] key);

}
