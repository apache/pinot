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
package org.apache.pinot.common.partition.function;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.segment.spi.partition.PartitionIdNormalizer;


/// Shared config-parsing helpers for built-in partition functions.
final class PartitionFunctionConfigs {
  /// Config key under which an explicit [PartitionIdNormalizer] can be selected.
  static final String PARTITION_ID_NORMALIZER_KEY = "partitionIdNormalizer";

  private PartitionFunctionConfigs() {
  }

  /// Reads [#PARTITION_ID_NORMALIZER_KEY] from the function config and resolves it to a
  /// [PartitionIdNormalizer]. Returns `defaultNormalizer` when the config is absent or
  /// the value is blank. Throws [IllegalArgumentException] on an unrecognized value.
  static PartitionIdNormalizer normalizer(@Nullable Map<String, String> functionConfig,
      PartitionIdNormalizer defaultNormalizer) {
    if (functionConfig == null) {
      return defaultNormalizer;
    }
    String raw = functionConfig.get(PARTITION_ID_NORMALIZER_KEY);
    if (StringUtils.isBlank(raw)) {
      return defaultNormalizer;
    }
    return PartitionIdNormalizer.fromConfigString(raw);
  }
}
