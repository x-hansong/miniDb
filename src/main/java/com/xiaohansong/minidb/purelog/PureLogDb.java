package com.xiaohansong.minidb.purelog;

import com.xiaohansong.minidb.MiniDb;
import com.xiaohansong.minidb.model.Constants;
import com.xiaohansong.minidb.model.HashIndexTable;
import com.xiaohansong.minidb.model.command.Command;
import com.xiaohansong.minidb.model.command.RmCommand;
import com.xiaohansong.minidb.model.command.SetCommand;
import com.xiaohansong.minidb.utils.LoggerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Comparator;
import java.util.TreeMap;

/**
 * 简单的基于日志的数据库
 */
public class PureLogDb implements MiniDb {

    private Logger LOGGER = LoggerFactory.getLogger(PureLogDb.class);


    private final TreeMap<String, HashIndexTable> indexMap;
    private HashIndexTable currentTable;
    private final long logSizeThreshold;

    public PureLogDb(String dataDir, long logSizeThreshold) {
        try {
            File dir = new File(dataDir);
            File[] files = dir.listFiles();
            indexMap = new TreeMap<>(Comparator.reverseOrder());
            currentTable = new HashIndexTable(newLogFileName());
            this.logSizeThreshold = logSizeThreshold;
            indexMap.put(currentTable.getLogName(), currentTable);
            if (files == null || files.length == 0) {
                return;
            }
            for (File file : files) {
                String fileName = file.getName();
                if (file.isFile() && fileName.endsWith(Constants.LOG_FILE_SUFFIX)) {
                    HashIndexTable indexTable = new HashIndexTable(file.getAbsolutePath());
                    indexMap.put(indexTable.getLogName(), indexTable);
                }
            }
            LoggerUtil.debug(LOGGER, "加载索引文件：{}", indexMap.keySet());
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private String newLogFileName() {
        return System.currentTimeMillis() + ".log";
    }

    @Override
    public void put(String key, String value) {
        try {
            SetCommand setCommand = new SetCommand(key, value);
            currentTable.writeCommand(setCommand);
            checkIfCreateNewLog();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public String get(String key) {
        try {
            for (HashIndexTable table : indexMap.values()) {
                Command command = table.queryCommand(key);
                LoggerUtil.debug(LOGGER, "查询key={}, 当前日志：{}, 结果：{}", key, table.getLogName(), command);
                if (command instanceof SetCommand) {
                    return ((SetCommand) command).getValue();
                } else if (command instanceof RmCommand) {
                    return null;
                }
            }
            return null;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public void remove(String key) {
        try {
            RmCommand rmCommand = new RmCommand(key);
            currentTable.writeCommand(rmCommand);
            checkIfCreateNewLog();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private void checkIfCreateNewLog() {
        long size = currentTable.logFileLength();
        if (size > logSizeThreshold) {
            currentTable = new HashIndexTable(newLogFileName());
            indexMap.put(currentTable.getLogName(), currentTable);
            LoggerUtil.debug(LOGGER, "当前文件长度为：{}, 超过阈值{},触发文件分段，新增日志：{}",
                    size, logSizeThreshold, currentTable.getLogName());
        }
    }

}
