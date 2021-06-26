package com.xiaohansong.minidb.model;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xiaohansong.minidb.model.command.Command;
import com.xiaohansong.minidb.model.command.CommandTypeEnum;
import com.xiaohansong.minidb.model.command.RmCommand;
import com.xiaohansong.minidb.model.command.SetCommand;
import com.xiaohansong.minidb.utils.LoggerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class HashIndexTable {

    private Logger LOGGER = LoggerFactory.getLogger(HashIndexTable.class);


    /**
     * 索引
     */
    private final Map<String, CommandPos> index;

    /**
     * 文件句柄
     */
    private final RandomAccessFile tableFile;

    /**
     * 文件
     */
    private final File file;

    public HashIndexTable(String filePath) {
        try {
            this.file = new File(filePath);
            this.tableFile = new RandomAccessFile(file, Constants.RW_MODE);
            this.index = new HashMap<>();
            //加载缓存
            tableFile.seek(0);
            while (tableFile.getFilePointer() < tableFile.length()) {
                int currentLength = tableFile.readInt();
                long offset = tableFile.getFilePointer();
                byte[] buffer = new byte[currentLength];
                tableFile.read(buffer);
                JSONObject commandJson = JSONObject.parseObject(new String(buffer, StandardCharsets.UTF_8));
                CommandPos commandPos = new CommandPos(offset, currentLength);
                index.put(commandJson.getString(Constants.KEY), commandPos);
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public Command queryCommand(String key) {
        try {
            CommandPos commandPos = index.get(key);
            if (commandPos == null) {
                return null;
            }
            tableFile.seek(commandPos.getOffset());
            byte[] commandBytes = new byte[(int) commandPos.getLength()];
            tableFile.read(commandBytes);
            JSONObject commandJson = JSON.parseObject(new String(commandBytes, StandardCharsets.UTF_8));
            if (CommandTypeEnum.SET.name().equals(commandJson.getString(Constants.TYPE))) {
                return commandJson.toJavaObject(SetCommand.class);
            } else if (CommandTypeEnum.RM.name().equals(commandJson.getString(Constants.TYPE))) {
                return commandJson.toJavaObject(RmCommand.class);
            }
            return null;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public void writeCommand(Command command) {
        try {
            tableFile.seek(tableFile.length());
            byte[] commandBytes = command.toString().getBytes(StandardCharsets.UTF_8);
            long length = commandBytes.length;
            tableFile.writeInt((int) length);
            long offset = tableFile.getFilePointer();
            tableFile.write(commandBytes);
            CommandPos commandPos = new CommandPos(offset, length);
            index.put(command.getKey(), commandPos);
            LoggerUtil.debug(LOGGER, "写入{}文件，当前内容为:{}", getLogName(), command);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

    }

    public String getLogName() {
        return file.getName();
    }

    public long logFileLength() {
        try {
            return tableFile.length();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }
}
