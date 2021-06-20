package com.xiaohansong.minidb.purelog;

import com.alibaba.fastjson.JSONObject;
import com.xiaohansong.minidb.MiniDb;
import com.xiaohansong.minidb.model.command.Command;
import com.xiaohansong.minidb.model.command.CommandTypeEnum;
import com.xiaohansong.minidb.model.command.RmCommand;
import com.xiaohansong.minidb.model.command.SetCommand;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * 简单的基于日志的数据库
 */
public class PureLogDb implements MiniDb {

    public static final String TYPE = "type";
    public static final String KEY = "key";
    private File logFile;
    private Map<String, Command> index;

    public PureLogDb(String logPath) {
        logFile = new File(logPath);
        index = new HashMap<>();
        loadIndex();
    }

    /**
     * 从日志文件加载数据到索引中
     */
    private void loadIndex() {
        try {
            if (!logFile.exists()) {
                return;
            }
            FileReader fileReader = new FileReader(logFile);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line = bufferedReader.readLine();
            while (line != null) {
                JSONObject command = JSONObject.parseObject(line);
                if (CommandTypeEnum.SET.name().equals(command.getString(TYPE))) {
                    SetCommand setCommand = command.toJavaObject(SetCommand.class);
                    index.put(setCommand.getKey(), setCommand);
                } else if (CommandTypeEnum.RM.name().equals(command.getString(TYPE))) {
                    RmCommand rmCommand = command.toJavaObject(RmCommand.class);
                    index.put(rmCommand.getKey(), rmCommand);
                }
                line = bufferedReader.readLine();
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public void put(String key, String value) {
        try {
            FileWriter fileWriter = new FileWriter(logFile, true);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            SetCommand setCommand = new SetCommand(key, value);
            bufferedWriter.write(setCommand.toString());
            bufferedWriter.newLine();
            bufferedWriter.close();
            index.put(setCommand.getKey(), setCommand);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public String get(String key) {
        try {
            Command command = index.get(key);
            if (command instanceof SetCommand) {
                return ((SetCommand) command).getValue();
            } else if (command instanceof RmCommand) {
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
            FileWriter fileWriter = new FileWriter(logFile, true);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            RmCommand rmCommand = new RmCommand(key);
            bufferedWriter.write(rmCommand.toString());
            bufferedWriter.newLine();
            bufferedWriter.close();
            index.put(rmCommand.getKey(), rmCommand);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

}
