package com.xiaohansong.minidb.purelog;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNull;

public class PureLogDbTest {

    @Test
    public void test() throws InterruptedException {
        PureLogDb pureLogDb = new PureLogDb(System.getProperty("user.dir") + "/", 1024);
        for (int i = 0; i < 10; i++) {
            pureLogDb.put("key" + i, "value" + i);
        }
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals("value" + i, pureLogDb.get("key" + i));
        }
        for (int i = 0; i < 10; i++) {
            pureLogDb.put("key" + i, "new_value" + i);
        }
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals("new_value" + i, pureLogDb.get("key" + i));
        }
        for (int i = 10; i < 20; i++) {
            assertNull(pureLogDb.get("key" + i));
        }
        for (int i = 0; i < 10; i++) {
            pureLogDb.remove("key" + i);
        }
        Thread.sleep(10000);
        for (int i = 0; i < 10; i++) {
            assertNull(pureLogDb.get("key" + i));
        }
    }

    @Test
    public void concurrentTest() throws InterruptedException {
        PureLogDb pureLogDb = new PureLogDb(System.getProperty("user.dir") + "/", 1024);
        ExecutorService executorService = Executors.newCachedThreadPool();
        List<Runnable> taskList = new ArrayList<>();
        Random random = new Random();
        Map<String, String> kv = new HashMap<>();

        PureLogDb finalPureLogDb = pureLogDb;
        for (int i = 0; i < 10; i++) {
            taskList.add(() -> {
                for (int j = 0; j < 100; j++) {
                    int r = random.nextInt();
                    String key = "key" + r;
                    String value = "value" + r;
                    finalPureLogDb.put(key, value);
                    Assert.assertEquals(value, finalPureLogDb.get(key));
                    if (r > 0) {
                        kv.put(key, value);
                    } else {
                        finalPureLogDb.remove(key);
                        Assert.assertEquals(null, finalPureLogDb.get(key));
                        kv.put(key, null);
                    }
                }
            });
        }
        taskList.forEach(executorService::execute);
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
        Thread.sleep(5000);

        pureLogDb = new PureLogDb(System.getProperty("user.dir") + "/", 1024);
        PureLogDb finalPureLogDb1 = pureLogDb;
        kv.forEach((key, value) -> {
            Assert.assertEquals(value, finalPureLogDb1.get(key));
        });
    }
}