package com.xiaohansong.minidb.model.command;

/**
 * 删除命令
 */
public class RmCommand extends AbstractCommand {

    /**
     * 数据key
     */
    private String key;

    public RmCommand(String key) {
        super(CommandTypeEnum.RM);
        this.key = key;
    }

    @Override
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}
