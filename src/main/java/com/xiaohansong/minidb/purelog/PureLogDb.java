package com.xiaohansong.minidb.purelog;

import com.xiaohansong.minidb.MiniDb;
import com.xiaohansong.minidb.model.Constants;
import com.xiaohansong.minidb.model.HashIndexTable;
import com.xiaohansong.minidb.model.LogMerger;
import com.xiaohansong.minidb.model.command.Command;
import com.xiaohansong.minidb.model.command.RmCommand;
import com.xiaohansong.minidb.model.command.SetCommand;
import com.xiaohansong.minidb.utils.LoggerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.Comparator;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 简单的基于日志的数据库
 */
public class PureLogDb implements MiniDb {

    private Logger LOGGER = LoggerFactory.getLogger(PureLogDb.class);


    private final TreeMap<String, HashIndexTable> indexMap;
    private HashIndexTable currentTable;
    private final long logSizeThreshold;

    private LogMerger logMerger;

    private String dataDir;

    public final Object dbLock;

    private ExecutorService compactThreadPool;

    public PureLogDb(String dataDir, long logSizeThreshold) {
        try {
            this.dataDir = dataDir;
            File dir = new File(dataDir);
            File[] files = dir.listFiles();
            indexMap = new TreeMap<>(Comparator.reverseOrder());
            currentTable = new HashIndexTable(newLogFileName());
            this.logSizeThreshold = logSizeThreshold;
            indexMap.put(currentTable.getUniqueName(), currentTable);
            logMerger = new LogMerger(this);
            dbLock = new Object();
            compactThreadPool = Executors.newFixedThreadPool(1);
            if (files == null || files.length == 0) {
                logMerger.start();
                return;
            }
            for (File file : files) {
                if (isLogFile(file)) {
                    HashIndexTable indexTable = new HashIndexTable(file.getAbsolutePath());
                    indexTable.compact();
                    indexMap.put(indexTable.getUniqueName(), indexTable);
                }
            }
            logMerger.start();
            LoggerUtil.debug(LOGGER, "已加载日志：{}", indexMap.keySet());
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private boolean isLogFile(File file) {
        return file.isFile() && (file.getName().endsWith(Constants.LOG_FILE_SUFFIX) ||
                file.getName().endsWith(Constants.COMPACTED_LOG_SUFFIX) ||
                file.getName().endsWith(Constants.MERGE_LOG_SUFFIX));
    }

    private String newLogFileName() {
        return dataDir + System.currentTimeMillis() + ".log";
    }

    @Override
    public void put(String key, String value) {
        try {
//            checkIfCreateNewLog();
            SetCommand setCommand = new SetCommand(key, value);
            currentTable.writeCommand(setCommand);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public String get(String key) {
        try {
            synchronized (this.dbLock) {
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
            }
        } catch (Throwable t) {
            throw new RuntimeException("key=" + key, t);
        }
    }

    @Override
    public void remove(String key) {
        try {
//            checkIfCreateNewLog();
            RmCommand rmCommand = new RmCommand(key);
            currentTable.writeCommand(rmCommand);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private void checkIfCreateNewLog() {
        long size = currentTable.logFileLength();
        if (size > logSizeThreshold) {
//            compactThreadPool.execute(() -> {
//                currentTable.compact();
//            });
            synchronized (dbLock) {
                currentTable = new HashIndexTable(newLogFileName());
                indexMap.put(currentTable.getUniqueName(), currentTable);
                LoggerUtil.debug(LOGGER, "当前文件长度为：{}, 超过阈值{},触发文件分段，新增日志：{}",
                        size, logSizeThreshold, currentTable.getLogName());
            }
        }
    }

    public Collection<HashIndexTable> getTables() {
        return indexMap.values();
    }

    public void removeTable(String uniqueName) {
        synchronized (dbLock) {
            indexMap.remove(uniqueName);
            LoggerUtil.debug(LOGGER, "删除log：{}，当前剩余log：{}", uniqueName, indexMap.keySet());
        }
    }

    public void putTable(HashIndexTable table) {
        synchronized (dbLock) {
            indexMap.put(table.getUniqueName(), table);
        }
    }

}
