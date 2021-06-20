package com.xiaohansong.minidb.model.command;

import com.alibaba.fastjson.JSON;

/**
 * 抽象命令
 */
public abstract class AbstractCommand implements Command {

    /**
     * 命令类型
     */
    private CommandTypeEnum type;

    public AbstractCommand(CommandTypeEnum type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    public CommandTypeEnum getType() {
        return type;
    }

    public void setType(CommandTypeEnum type) {
        this.type = type;
    }
}
