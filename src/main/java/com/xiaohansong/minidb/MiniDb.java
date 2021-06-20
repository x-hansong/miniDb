package com.xiaohansong.minidb;

/**
 * 数据库接口
 */
public interface MiniDb {

    void put(String key, String value);

    String get(String key);

    void remove(String key);

}
