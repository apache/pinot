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
package org.apache.pinot.segment.local.upsert.merger;

import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.config.table.UpsertConfig;


public class PartialUpsertMergerFactory {

  private PartialUpsertMergerFactory() {
  }

  /**
   * Initialise the default partial upsert merger or initialise a custom implementation from a given class name in
   * config
   * @param primaryKeyColumns
   * @param comparisonColumns
   * @param upsertConfig
   * @return
   */
  public static PartialUpsertMerger getPartialUpsertMerger(List<String> primaryKeyColumns,
      List<String> comparisonColumns, UpsertConfig upsertConfig) {
    PartialUpsertMerger partialUpsertMerger;
    String customRowMerger = upsertConfig.getPartialUpsertMergerClass();
    // If a custom implementation is provided in config, initialize an implementation and return.
    if (StringUtils.isNotBlank(customRowMerger)) {
      try {
        Class<?> partialUpsertMergerClass = Class.forName(customRowMerger);
        if (!BasePartialUpsertMerger.class.isAssignableFrom(partialUpsertMergerClass)) {
          throw new RuntimeException(
              "Provided partialUpsertMergerClass is not an implementation of BasePartialUpsertMerger.class");
        }
        partialUpsertMerger =
            (PartialUpsertMerger) partialUpsertMergerClass.getConstructor(List.class, List.class, UpsertConfig.class)
                .newInstance(primaryKeyColumns, comparisonColumns, upsertConfig);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Could not load partial upsert implementation class by name %s", customRowMerger), e);
      }
    } else {
      // return default implementation
      partialUpsertMerger = new PartialUpsertColumnarMerger(primaryKeyColumns, comparisonColumns, upsertConfig);
    }
    return partialUpsertMerger;
  }
}
