package org.apache.pinot.common.upsert.hash;

import org.apache.pinot.spi.data.readers.PrimaryKey;

public interface UpsertHashFunction {
  Object hash(PrimaryKey primaryKey);

  String getId();
}
