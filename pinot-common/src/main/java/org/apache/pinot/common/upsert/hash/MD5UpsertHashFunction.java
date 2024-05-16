package org.apache.pinot.common.upsert.hash;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hashing;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.ByteArray;


public class MD5UpsertHashFunction implements UpsertHashFunction {
  public static final MD5UpsertHashFunction INSTANCE = new MD5UpsertHashFunction();

  @Override
  public Object hash(PrimaryKey primaryKey) {
    return new ByteArray(hashMD5(primaryKey.asBytes()));
  }

  @Override
  public String getId() {
    return "MD5";
  }

  @VisibleForTesting
  static byte[] hashMD5(byte[] bytes) {
    return Hashing.md5().hashBytes(bytes).asBytes();
  }
}
