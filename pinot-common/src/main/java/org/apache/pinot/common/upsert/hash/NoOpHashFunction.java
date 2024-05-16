package org.apache.pinot.common.upsert.hash;

import org.apache.pinot.spi.data.readers.PrimaryKey;


public class NoOpHashFunction implements UpsertHashFunction {
  public static final NoOpHashFunction INSTANCE = new NoOpHashFunction();

  @Override
  public Object hash(PrimaryKey primaryKey) {
    return primaryKey;
  }

  @Override
  public String getId() {
    return "NONE";
  }
}
