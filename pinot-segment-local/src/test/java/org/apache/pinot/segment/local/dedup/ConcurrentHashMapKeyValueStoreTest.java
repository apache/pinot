/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.dedup;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.spi.ingestion.dedup.LocalKeyValueStore;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ConcurrentHashMapKeyValueStoreTest {

    LocalKeyValueStore _keyValueStore = new ConcurrentHashMapKeyValueStore("test".getBytes(StandardCharsets.UTF_8));
    byte[] _nonExistingKey = "non-existing".getBytes();
    byte[] _key = "someKey".getBytes();
    byte[] _key2 = "someKey2".getBytes();

    @BeforeMethod
    public void setUp() throws Exception {
        cleanup();
    }

    private void cleanup() throws Exception {
        List<byte[]> allKeys = Arrays.asList(_key, _key2, _nonExistingKey);
        allKeys.forEach(aKey -> _keyValueStore.delete(aKey));
        _keyValueStore.compact();
    }

    @AfterMethod
    public void tearDown() throws Exception {
        cleanup();
    }

    @Test
    public void testGetWithNonExistingKey() {
        Assert.assertNull(_keyValueStore.get(_nonExistingKey));
    }

    @Test
    public void testPut() {
        byte[] value = "someValue".getBytes();
        Assert.assertNull(_keyValueStore.get(_key));
        _keyValueStore.put(_key, value);
        Assert.assertEquals(_keyValueStore.get(_key), value);
    }

    @Test
    public void testDelete() {
        byte[] value = "someValue".getBytes();
        Assert.assertNull(_keyValueStore.get(_key));
        _keyValueStore.put(_key, value);
        Assert.assertEquals(_keyValueStore.get(_key), value);
        _keyValueStore.delete(_key);
        Assert.assertNull(_keyValueStore.get(_key));
    }

    @Test
    public void testPutBatch() {
        byte[] value = "someValue".getBytes();
        Assert.assertNull(_keyValueStore.get(_key));
        Assert.assertNull(_keyValueStore.get(_key2));
        List<Pair<byte[], byte[]>> batch = Arrays.asList(Pair.of(_key, value), Pair.of(_key2, value));
        _keyValueStore.putBatch(batch);
        Assert.assertEquals(_keyValueStore.get(_key), value);
        Assert.assertEquals(_keyValueStore.get(_key2), value);
    }

    @Test
    public void testGetKeyCount() {
        byte[] value = "someValue".getBytes();
        Assert.assertNull(_keyValueStore.get(_key));
        _keyValueStore.put(_key, value);
        Assert.assertEquals(_keyValueStore.get(_key), value);
        Assert.assertEquals(_keyValueStore.getKeyCount(), 1);
        _keyValueStore.put(_key2, value);
        Assert.assertEquals(_keyValueStore.get(_key2), value);
        Assert.assertEquals(_keyValueStore.getKeyCount(), 2);
    }
}
