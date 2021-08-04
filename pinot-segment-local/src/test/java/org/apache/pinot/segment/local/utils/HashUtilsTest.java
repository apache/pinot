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
package org.apache.pinot.segment.local.utils;

import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class HashUtilsTest {
  @Test
  public void testHashPlainValues() {
    Assert.assertEquals(BytesUtils.toHexString(HashUtils.hashMD5("hello world".getBytes())),
        "5eb63bbbe01eeed093cb22bb8f5acdc3");
    Assert.assertEquals(BytesUtils.toHexString(HashUtils.hashMurmur3("hello world".getBytes())),
        "0e617feb46603f53b163eb607d4697ab");
  }

  @Test
  public void testHashPrimaryKey() {
    PrimaryKey pk = new PrimaryKey(new Object[]{"uuid-1", "uuid-2", "uuid-3"});
    Assert.assertEquals(
        BytesUtils.toHexString(((ByteArray) HashUtils.hashPrimaryKey(pk, UpsertConfig.HashFunction.MD5)).getBytes()),
        "58de44997505014e02982846a4d1cbbd");
    Assert.assertEquals(BytesUtils
            .toHexString(((ByteArray) HashUtils.hashPrimaryKey(pk, UpsertConfig.HashFunction.MURMUR3)).getBytes()),
        "7e6b4a98296292a4012225fff037fa8c");
    // reorder
    pk = new PrimaryKey(new Object[]{"uuid-3", "uuid-2", "uuid-1"});
    Assert.assertEquals(
        BytesUtils.toHexString(((ByteArray) HashUtils.hashPrimaryKey(pk, UpsertConfig.HashFunction.MD5)).getBytes()),
        "d2df12c6dea7b83f965613614eee58e2");
    Assert.assertEquals(BytesUtils
            .toHexString(((ByteArray) HashUtils.hashPrimaryKey(pk, UpsertConfig.HashFunction.MURMUR3)).getBytes()),
        "8d68b314cc0c8de4dbd55f4dad3c3e66");
  }
}
