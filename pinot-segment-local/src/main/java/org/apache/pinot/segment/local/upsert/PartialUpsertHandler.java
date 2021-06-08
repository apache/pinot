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
package org.apache.pinot.segment.local.upsert;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.segment.local.upsert.merger.PartialUpsertMerger;
import org.apache.pinot.segment.local.upsert.merger.PartialUpsertMergerFactory;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.readers.GenericRow;


public class PartialUpsertHandler {
  private final Map<String, PartialUpsertMerger> _mergers = new HashMap<>();

  /**
   * Initializes the partial upsert merger with upsert config. Different fields can have different merge strategies.
   *
   * @param partialUpsertStrategies can be derived into fields to merger map.
   */
  public PartialUpsertHandler(Map<String, UpsertConfig.Strategy> partialUpsertStrategies) {
    for (Map.Entry<String, UpsertConfig.Strategy> entry : partialUpsertStrategies.entrySet()) {
      _mergers.put(entry.getKey(), PartialUpsertMergerFactory.getMerger(entry.getValue()));
    }
  }

  /**
   * Handle partial upsert merge for given fieldName.
   *
   * @param previousRecord the last derived full record during ingestion.
   * @param newRecord the new consumed record.
   * @return a new row after merge
   */
  public GenericRow merge(GenericRow previousRecord, GenericRow newRecord) {
    for (Map.Entry<String, PartialUpsertMerger> entry : _mergers.entrySet()) {
      String column = entry.getKey();
      if (!previousRecord.isNullValue(column)) {
        if (newRecord.isNullValue(column)) {
          newRecord.putValue(column, previousRecord.getValue(column));
          newRecord.removeNullValueField(column);
        } else {
          newRecord
              .putValue(column, entry.getValue().merge(previousRecord.getValue(column), newRecord.getValue(column)));
        }
      }
    }
    return newRecord;
  }
}
