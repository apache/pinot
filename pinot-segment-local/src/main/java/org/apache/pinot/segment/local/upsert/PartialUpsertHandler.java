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
import org.apache.pinot.segment.local.upsert.merger.IncrementMerger;
import org.apache.pinot.segment.local.upsert.merger.PartialUpsertMerger;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.readers.GenericRow;


public class PartialUpsertHandler {
  private HashMap<String, PartialUpsertMerger> _mergers;
  private final UpsertConfig.STRATEGY _defaultStrategy = UpsertConfig.STRATEGY.OVERWRITE;

  /**
   * Initializes the partial upsert merger with upsert config. Different fields can have different merge strategies.
   *
   * @param partialUpsertStrategies can be derived into fields to merger map.
   */
  public void init(Map<String, UpsertConfig.STRATEGY> partialUpsertStrategies) {
    _mergers = new HashMap<>();

    for (String fieldName : partialUpsertStrategies.keySet()) {
      UpsertConfig.STRATEGY strategy = partialUpsertStrategies.get(fieldName);
      if (strategy != _defaultStrategy) {
        if (strategy == UpsertConfig.STRATEGY.INCREMENT) {
          _mergers.put(fieldName, new IncrementMerger());
        }
      }
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
    for (String fieldName : _mergers.keySet()) {
      if (!previousRecord.isNullValue(fieldName)) {
        if (newRecord.isNullValue(fieldName)) {
          newRecord.putValue(fieldName, previousRecord.getValue(fieldName));
        } else {
          Object newValue =
              _mergers.get(fieldName).merge(previousRecord.getValue(fieldName), newRecord.getValue(fieldName));
          newRecord.putValue(fieldName, newValue);
        }
      }
    }
    return newRecord;
  }
}
