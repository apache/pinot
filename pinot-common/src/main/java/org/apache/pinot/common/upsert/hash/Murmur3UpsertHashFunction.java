package org.apache.pinot.common.upsert.hash;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hashing;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.ByteArray;


public class Murmur3UpsertHashFunction implements UpsertHashFunction {
  public static final Murmur3UpsertHashFunction INSTANCE = new Murmur3UpsertHashFunction();

  @Override
  public Object hash(PrimaryKey primaryKey) {
    return new ByteArray(hashMurmur3(primaryKey.asBytes()));
  }

  @Override
  public String getId() {
    return "MURMUR3";
  }

  @VisibleForTesting
  static byte[] hashMurmur3(byte[] bytes) {
    return Hashing.murmur3_128().hashBytes(bytes).asBytes();
  }
}
