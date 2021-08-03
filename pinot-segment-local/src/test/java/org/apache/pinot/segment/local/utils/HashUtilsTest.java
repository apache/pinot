package org.apache.pinot.segment.local.utils;

import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.readers.PrimaryKey;
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
    Assert.assertEquals(BytesUtils.toHexString((byte[]) HashUtils.hashPrimaryKey(pk, UpsertConfig.HashFunction.MD5)),
        "58de44997505014e02982846a4d1cbbd");
    Assert
        .assertEquals(BytesUtils.toHexString((byte[]) HashUtils.hashPrimaryKey(pk, UpsertConfig.HashFunction.MURMUR3)),
            "7e6b4a98296292a4012225fff037fa8c");
    // reorder
    pk = new PrimaryKey(new Object[]{"uuid-3", "uuid-2", "uuid-1"});
    Assert.assertEquals(BytesUtils.toHexString((byte[]) HashUtils.hashPrimaryKey(pk, UpsertConfig.HashFunction.MD5)),
        "d2df12c6dea7b83f965613614eee58e2");
    Assert
        .assertEquals(BytesUtils.toHexString((byte[]) HashUtils.hashPrimaryKey(pk, UpsertConfig.HashFunction.MURMUR3)),
            "8d68b314cc0c8de4dbd55f4dad3c3e66");
  }
}
