package com.xiaohansong.minidb.model.command;

/**
 * 保存命令
 */
public class SetCommand extends AbstractCommand {

    /**
     * 数据key
     */
    private String key;

    /**
     * 数据值
     */
    private String value;

    public SetCommand(String key, String value) {
        super(CommandTypeEnum.SET);
        this.key = key;
        this.value = value;
    }

    @Override
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
