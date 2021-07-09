package com.xiaohansong.minidb.model;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.xiaohansong.minidb.model.command.Command;
import com.xiaohansong.minidb.model.command.CommandTypeEnum;
import com.xiaohansong.minidb.model.command.RmCommand;
import com.xiaohansong.minidb.model.command.SetCommand;
import com.xiaohansong.minidb.utils.LoggerUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HashIndexTable {

    private Logger LOGGER = LoggerFactory.getLogger(HashIndexTable.class);


    /**
     * 索引
     */
    private Map<String, CommandPos> index;

    /**
     * 文件句柄
     */
    private RandomAccessFile tableFile;

    /**
     * 文件
     */
    private File file;

    /**
     * 索引文件
     */
    private File indexFile;

    public HashIndexTable(String filePath) {
        this(filePath, true);
    }

    public HashIndexTable(String filePath, boolean loadFromIndex) {
        try {
            this.file = new File(filePath);
            this.tableFile = new RandomAccessFile(file, Constants.RW_MODE);
            this.index = new HashMap<>();
            this.indexFile = new File(getIndexFilePath());
            if (indexFile.exists() && loadFromIndex) {
                JSONObject indexJson = JSONObject.parseObject(new FileInputStream(indexFile), JSONObject.class);
                index = indexJson.toJavaObject(new TypeReference<Map<String, CommandPos>>() {});
                LoggerUtil.debug(LOGGER, "加载索引文件：{}", indexFile.getAbsolutePath());
            } else {
                loadIndexFromLog();
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private void loadIndexFromLog() throws IOException {
        if (tableFile.length() == 0) {
            return;
        }
        LoggerUtil.debug(LOGGER, "从日志文件加载索引：{}", file.getAbsolutePath());
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
            CommandPos commandPos = writeToFile(tableFile, command);
            index.put(command.getKey(), commandPos);
            LoggerUtil.debug(LOGGER, "写入{}文件，当前内容为:{}", getLogName(), command);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public void compact() {
        try {
            if (isCompacted()) {
                return;
            }
            Map<String, CommandPos> compactIndex = new HashMap<>();
            File compactFile = new File(getCompactLogPath());
            RandomAccessFile compactLog = new RandomAccessFile(
                    compactFile, Constants.RW_MODE);
            for (String key : index.keySet()) {
                Command command = queryCommand(key);
                CommandPos commandPos = writeToFile(compactLog, command);
                compactIndex.put(command.getKey(), commandPos);
            }
            long beforeCompactedSize = tableFile.length();
            tableFile.close();
            if (!file.delete()) {
                throw new RuntimeException("删除文件失败：" + file.getName());
            }
            tableFile = compactLog;
            file = compactFile;
            index = compactIndex;
            long afterCompactedSize = tableFile.length();
            LoggerUtil.debug(LOGGER, "压缩日志文件成功: {}，压缩前大小：{}，压缩后大小：{}",
                    file.getName(), beforeCompactedSize, afterCompactedSize);
            buildHashIndexFile();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private CommandPos writeToFile(RandomAccessFile wal, Command command) {
        try {
            wal.seek(wal.length());
            byte[] commandBytes = command.toString().getBytes(StandardCharsets.UTF_8);
            long length = commandBytes.length;
            wal.writeInt((int) length);
            long offset = wal.getFilePointer();
            wal.write(commandBytes);
            return new CommandPos(offset, length);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public String getUniqueName() {
        return StringUtils.substringBefore(file.getName(), Constants.LOG_FILE_SUFFIX);
    }

    public String getLogName() {
        return file.getName();
    }

    public String getCompactLogPath() {
        return getDataDir() + getLogName() + Constants.COMPACTED_LOG_SUFFIX;
    }

    public String getMergeLogPath() {
        return getDataDir() + getLogName() + Constants.MERGE_LOG_SUFFIX;
    }

    public boolean isCompacted() {
        return getLogName().contains(Constants.COMPACTED_LOG_SUFFIX);
    }

    public boolean isMerged() {
        return getLogName().contains(Constants.MERGE_LOG_SUFFIX);
    }

    public long logFileLength() {
        try {
            return tableFile.length();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public List<Command> getAllCommand() {
        List<Command> commands = new ArrayList<>();
        for (String key : index.keySet()) {
            Command command = queryCommand(key);
            commands.add(command);
        }
        return commands;
    }

    public void delete() {
        try {
            tableFile.close();
            if (!file.delete()) {
                throw new RuntimeException("删除文件失败：" + file.getName());
            }
            if (indexFile != null && indexFile.exists() && !indexFile.delete()) {
                throw new RuntimeException("删除文件失败：" + file.getName());
            }

        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public String getDataDir() {
        return file.getParent() + "/";
    }

    public String getIndexFileName() {
        return getUniqueName() + Constants.INDEX_SUFFIX;
    }

    public String getIndexFilePath() {
        return getDataDir() + getIndexFileName();
    }

    public void buildHashIndexFile() {
        try {
            indexFile = new File(getIndexFilePath());
            if (indexFile.exists()) {
                indexFile.delete();
            }
            RandomAccessFile indexWriter = new RandomAccessFile(indexFile,
                    Constants.RW_MODE);
            byte[] indexBytes = JSONObject.toJSONString(index).getBytes(StandardCharsets.UTF_8);
            indexWriter.write(indexBytes);
            indexWriter.close();
            LoggerUtil.debug(LOGGER, "构建索引文件：{}", indexFile.getAbsolutePath());
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }
}
