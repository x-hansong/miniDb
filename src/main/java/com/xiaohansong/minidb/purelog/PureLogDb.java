package com.xiaohansong.minidb.purelog;

import com.xiaohansong.minidb.MiniDb;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * 简单的基于日志的数据库
 */
public class PureLogDb implements MiniDb {

    private File logFile;

    private byte[] fieldSeparator = ":".getBytes(StandardCharsets.UTF_8);

    private byte[] kvSeparator = "\n".getBytes(StandardCharsets.UTF_8);

    public PureLogDb(String logPath) {
        logFile = new File(logPath);
    }

    @Override
    public void put(byte[] key, byte[] value) {
        try {
            FileOutputStream outputStream = new FileOutputStream(logFile);
            outputStream.write(key);
            outputStream.write(fieldSeparator);
            outputStream.write(value);
            outputStream.write(kvSeparator);
            outputStream.close();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

    }

    @Override
    public byte[] get(byte[] key) {
        try {

        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
        return new byte[0];
    }

    @Override
    public void delete(byte[] key) {

    }
}
