package com.xiaohansong.minidb.purelog;

import com.alibaba.fastjson.JSONObject;
import com.xiaohansong.minidb.MiniDb;
import com.xiaohansong.minidb.model.command.Command;
import com.xiaohansong.minidb.model.command.CommandTypeEnum;
import com.xiaohansong.minidb.model.command.RmCommand;
import com.xiaohansong.minidb.model.command.SetCommand;

import java.io.*;
import java.util.LinkedList;

/**
 * 简单的基于日志的数据库
 */
public class PureLogDb implements MiniDb {

    public static final String TYPE = "type";
    public static final String KEY = "key";
    private File logFile;

    public PureLogDb(String logPath) {
        logFile = new File(logPath);
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
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public String get(String key) {
        try {
            FileReader fileReader = new FileReader(logFile);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line = bufferedReader.readLine();
            LinkedList<Command> commands = new LinkedList<>();
            while (line != null) {
                JSONObject command = JSONObject.parseObject(line);
                if (key.equals(command.getString(KEY))) {
                    if (CommandTypeEnum.SET.name().equals(command.getString(TYPE))) {
                        commands.add(command.toJavaObject(SetCommand.class));
                    } else if (CommandTypeEnum.RM.name().equals(command.getString(TYPE))) {
                        commands.add(command.toJavaObject(RmCommand.class));
                    }
                }
                line = bufferedReader.readLine();
            }
            if (commands.size() != 0) {
                if (commands.getLast() instanceof SetCommand) {
                    return ((SetCommand) commands.getLast()).getValue();
                } else if (commands.getLast() instanceof RmCommand) {
                    return null;
                }
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
        return null;
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
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

}
