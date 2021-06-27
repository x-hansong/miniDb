package com.xiaohansong.minidb.model;

import com.xiaohansong.minidb.model.command.Command;
import com.xiaohansong.minidb.purelog.PureLogDb;
import com.xiaohansong.minidb.utils.LoggerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LogMerger extends Thread {

    private Logger LOGGER = LoggerFactory.getLogger(LogMerger.class);

    private PureLogDb pureLogDb;

    public LogMerger(PureLogDb pureLogDb) {
        this.pureLogDb = pureLogDb;
        this.setDaemon(true);
        this.setName("LogCompactor");
    }

    @Override
    public void run() {
        try {
            while (true) {
                LinkedList<HashIndexTable> tables = pureLogDb.getTables()
                        .stream().filter(table -> table.isCompacted() || table.isMerged())
                        .collect(Collectors.toCollection(LinkedList::new));
                List<String> mergeLogNames = tables.stream().map(HashIndexTable::getLogName)
                        .collect(Collectors.toList());
                LoggerUtil.debug(LOGGER, "待合并日志：{}", mergeLogNames);
                if (tables.size() <= 1) {
                    Thread.sleep(3000);
                    LoggerUtil.debug(LOGGER, "待合并日志少于两个，无需合并：{}", mergeLogNames);
                    continue;
                }
                HashIndexTable firstTable = tables.getFirst();
                HashIndexTable mergeTable = new HashIndexTable(firstTable.getMergeLogName());
                Map<String, Command> mergeIndex = new HashMap<>();
                for (HashIndexTable table : tables) {
                    List<Command> commandList = table.getAllCommand();
                    for (Command command : commandList) {
                        mergeIndex.putIfAbsent(command.getKey(), command);
                    }
                }
                for (Command command : mergeIndex.values()) {
                    mergeTable.writeCommand(command);
                }
                pureLogDb.putTable(mergeTable);
                for (HashIndexTable table : tables) {
                    pureLogDb.removeTable(table.getUniqueName());
                    table.delete();
                }
                LoggerUtil.debug(LOGGER, "日志合并完成: {}", mergeTable.getLogName());
                Thread.sleep(3000);
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }
}