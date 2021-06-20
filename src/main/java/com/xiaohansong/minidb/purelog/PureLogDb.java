package com.xiaohansong.minidb.purelog;

import com.alibaba.fastjson.JSONObject;
import com.xiaohansong.minidb.MiniDb;

import java.io.*;
import java.util.LinkedList;

/**
 * 简单的基于日志的数据库
 */
public class PureLogDb implements MiniDb {

    private File logFile;

    public PureLogDb(String logPath) {
        logFile = new File(logPath);
    }

    @Override
    public void put(String key, String value) {
        try {
            FileWriter fileWriter = new FileWriter(logFile, true);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            JSONObject kv = new JSONObject();
            kv.put(key, value);
            bufferedWriter.write(kv.toJSONString());
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
            LinkedList<String> values = new LinkedList<>();
            while (line != null) {
                JSONObject kv = JSONObject.parseObject(line);
                if (kv.getString(key) != null) {
                    values.add(kv.getString(key));
                }
                line = bufferedReader.readLine();
            }
            if (values.size() != 0) {
                return values.getLast();
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
        return null;
    }

}
