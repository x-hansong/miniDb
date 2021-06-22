package com.xiaohansong.minidb.purelog;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xiaohansong.minidb.MiniDb;
import com.xiaohansong.minidb.model.CommandPos;
import com.xiaohansong.minidb.model.command.CommandTypeEnum;
import com.xiaohansong.minidb.model.command.RmCommand;
import com.xiaohansong.minidb.model.command.SetCommand;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * 简单的基于日志的数据库
 */
public class PureLogDb implements MiniDb {

    public static final String TYPE = "type";
    public static final String KEY = "key";
    public static final String RW_MODE = "rw";
    public static final char COMMAND_SEPARATOR = '\n';
    public static final String LINE_BREAK = "\n";
    private File logFile;
    private RandomAccessFile wal;
    private long currentOffset;
    private Map<String, CommandPos> index;

    public PureLogDb(String logPath) {
        try {
            logFile = new File(logPath);
            wal = new RandomAccessFile(logFile, RW_MODE);
            currentOffset = wal.length();
            index = new HashMap<>();
            loadIndex();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * 从日志文件加载数据到索引中
     */
    private void loadIndex() {
        try {
            wal.seek(0);
            while (wal.getFilePointer() < currentOffset) {
                int currentLength = wal.readInt();
                long offset = wal.getFilePointer();
                byte[] buffer = new byte[currentLength];
                wal.read(buffer);
                JSONObject commandJson = JSONObject.parseObject(new String(buffer, StandardCharsets.UTF_8));
                CommandPos commandPos = new CommandPos(offset, currentLength);
                index.put(commandJson.getString(KEY), commandPos);
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public void put(String key, String value) {
        try {
            checkParam(key);
            checkParam(value);
            SetCommand setCommand = new SetCommand(key, value);
            wal.seek(currentOffset);
            byte[] commandBytes = setCommand.toString().getBytes(StandardCharsets.UTF_8);
            long length = commandBytes.length;
            wal.writeInt((int) length);
            long offset = wal.getFilePointer();
            wal.write(commandBytes);
            currentOffset = wal.getFilePointer();
            CommandPos commandPos = new CommandPos(offset, length);
            index.put(setCommand.getKey(), commandPos);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public String get(String key) {
        try {
            CommandPos commandPos = index.get(key);
            if (commandPos == null) {
                return null;
            }
            wal.seek(commandPos.getOffset());
            byte[] commandBytes = new byte[(int) commandPos.getLength()];
            wal.read(commandBytes);
            JSONObject commandJson = JSON.parseObject(new String(commandBytes, StandardCharsets.UTF_8));
            if (CommandTypeEnum.SET.name().equals(commandJson.getString(TYPE))) {
                SetCommand setCommand = commandJson.toJavaObject(SetCommand.class);
                return setCommand.getValue();
            } else if (CommandTypeEnum.RM.name().equals(commandJson.getString(TYPE))) {
                return null;
            }
            return null;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public void remove(String key) {
        try {
            checkParam(key);
            RmCommand rmCommand = new RmCommand(key);
            wal.seek(currentOffset);
            byte[] commandBytes = rmCommand.toString().getBytes(StandardCharsets.UTF_8);
            long length = commandBytes.length;
            wal.writeInt((int) length);
            long offset = wal.getFilePointer();
            wal.write(commandBytes);
            currentOffset = wal.getFilePointer();
            CommandPos commandPos = new CommandPos(offset, length);
            index.put(rmCommand.getKey(), commandPos);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * 检查入参
     * @param input
     */
    private void checkParam(String input) {
        if (input == null) {
            throw new IllegalArgumentException("参数不能为null");
        }

        if (input.contains(LINE_BREAK)) {
            throw new IllegalArgumentException("参数不能包含换行符");
        }
    }

}
