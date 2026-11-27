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
package org.apache.pinot.segment.spi.index.creator;

import java.io.IOException;
import java.math.BigDecimal;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.BytesUtils;


public interface BloomFilterCreator extends IndexCreator {

  FieldSpec.DataType getDataType();

  /// Returns the string that represents a BIG_DECIMAL value in the bloom filter. Numerically equal values can have
  /// different string forms (`1.50` vs `1.5`, `100` vs `1E+2`), so the value is normalized the same way on the
  /// creation side and on the query side.
  static String toBloomFilterString(BigDecimal value) {
    return value.stripTrailingZeros().toPlainString();
  }

  @Override
  default void add(Object value, int dictId) {
    add(valueToString(value));
  }

  @Override
  default void add(Object[] values, @Nullable int[] dictIds) {
    for (Object value : values) {
      add(valueToString(value));
    }
  }

  private String valueToString(Object value) {
    switch (getDataType().getStoredType()) {
      case BYTES:
        return BytesUtils.toHexString((byte[]) value);
      case BIG_DECIMAL:
        return toBloomFilterString((BigDecimal) value);
      default:
        return value.toString();
    }
  }

  /// Adds a value to the bloom filter.
  void add(String value);

  /// Seals the index and flushes it to disk.
  void seal()
      throws IOException;
}
