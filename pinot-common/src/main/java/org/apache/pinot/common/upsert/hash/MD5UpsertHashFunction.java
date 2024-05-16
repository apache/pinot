package org.apache.pinot.common.upsert.hash;

import com.google.common.hash.Hashing;
import org.apache.pinot.spi.data.readers.PrimaryKey;


public class MD5UpsertHashFunction implements UpsertHashFunction {
  public static final MD5UpsertHashFunction INSTANCE = new MD5UpsertHashFunction();

  @Override
  public Object hash(PrimaryKey primaryKey) {
    return Hashing.md5().hashBytes(primaryKey.asBytes()).asBytes();
  }

  @Override
  public String getId() {
    return "MD5";
  }
}
