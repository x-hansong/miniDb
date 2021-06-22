package com.xiaohansong.minidb.model;

/**
 * 命令的位置信息
 */
public class CommandPos extends ToJSONString {

    /**
     * 偏移量
     */
    private long offset;

    /**
     * 长度
     */
    private long length;

    public CommandPos(long offset, long length) {
        this.offset = offset;
        this.length = length;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }
}
