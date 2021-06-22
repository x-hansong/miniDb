package com.xiaohansong.minidb.model;

import com.alibaba.fastjson.JSON;

import java.io.Serializable;

public abstract class ToJSONString implements Serializable {
    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
