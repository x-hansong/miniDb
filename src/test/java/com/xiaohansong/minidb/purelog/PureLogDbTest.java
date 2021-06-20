package com.xiaohansong.minidb.purelog;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class PureLogDbTest {

    @Test
    public void test() {
        PureLogDb pureLogDb = new PureLogDb("db.log");
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
        for (int i = 0; i < 10; i++) {
            assertNull(pureLogDb.get("key" + i));
        }
    }
}