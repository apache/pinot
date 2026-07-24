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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;


/**
 * Configuration for the null value vector index of a column.
 *
 * The {@code enabled} state is derived from null handling (schema column-based nullability or the table-level
 * {@code nullHandlingEnabled} flag) rather than set directly, so users typically only configure {@code backfill}.
 *
 * When {@code backfill} is {@code true}, segment reload generates a null value vector for the column if it has null
 * handling enabled but no null value vector yet, by treating every value equal to the column's default null value as
 * null. This is a lossy reconstruction (a genuine value equal to the default is also marked null), so it is per-column
 * opt-in — enable it only for columns whose default null value is a sentinel that does not occur in the data (e.g. a
 * dimension's {@code MIN_VALUE}), not for metric/boolean columns whose default is a common value like {@code 0}.
 */
public class NullValueVectorConfig extends IndexConfig {
  private final boolean _backfill;

  @JsonCreator
  public NullValueVectorConfig(@JsonProperty("disabled") Boolean disabled,
      @JsonProperty("backfill") boolean backfill) {
    super(disabled);
    _backfill = backfill;
  }

  public boolean isBackfill() {
    return _backfill;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!super.equals(o)) {
      return false;
    }
    return _backfill == ((NullValueVectorConfig) o)._backfill;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _backfill);
  }
}
