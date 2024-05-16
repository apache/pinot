package org.apache.pinot.common.upsert.hash;

import com.google.common.hash.Hashing;
import org.apache.pinot.spi.data.readers.PrimaryKey;


public class Murmur3UpsertHashFunction implements UpsertHashFunction {
  public static final Murmur3UpsertHashFunction INSTANCE = new Murmur3UpsertHashFunction();

  @Override
  public Object hash(PrimaryKey primaryKey) {
    return Hashing.murmur3_128().hashBytes(primaryKey.asBytes()).asBytes();
  }

  @Override
  public String getId() {
    return "MURMUR3";
  }
}
