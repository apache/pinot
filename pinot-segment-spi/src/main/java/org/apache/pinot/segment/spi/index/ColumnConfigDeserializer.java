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

package org.apache.pinot.segment.spi.index;

import java.util.Collections;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


@FunctionalInterface
public interface ColumnConfigDeserializer<C> {

  /**
   * This method is called to extract the configuration from user configuration for all columns.
   */
  Map<String, C> deserialize(TableConfig tableConfig, Schema schema);

  /**
   * Returns a new {@link ColumnConfigDeserializer} that will merge results from the receiver deserializer and the one
   * received as parameter, failing if a column is defined in both.
   */
  default ColumnConfigDeserializer<C> withExclusiveAlternative(ColumnConfigDeserializer<C> alternative) {
    return new MergedColumnConfigDeserializer<>(MergedColumnConfigDeserializer.OnConflict.FAIL, this, alternative);
  }

  /**
   * Returns a new {@link ColumnConfigDeserializer} that will merge results from the receiver deserializer and the one
   * received as parameter, giving priority to the ones in the receiver if a column is defined in both.
   */
  default ColumnConfigDeserializer<C> withFallbackAlternative(ColumnConfigDeserializer<C> alternative) {
    return new MergedColumnConfigDeserializer<>(MergedColumnConfigDeserializer.OnConflict.PICK_FIRST, this,
        alternative);
  }

  static <C extends IndexConfig> ColumnConfigDeserializer<C> onlyIf(Predicate<TableConfig> predicate,
      ColumnConfigDeserializer<C> delegate) {
    return ((tableConfig, schema) -> predicate.test(tableConfig)
        ? delegate.deserialize(tableConfig, schema)
        : Collections.emptyMap());
  }
}
