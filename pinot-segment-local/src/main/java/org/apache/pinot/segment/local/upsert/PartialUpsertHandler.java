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
import org.apache.pinot.segment.local.upsert.merger.IgnoreMerger;
import org.apache.pinot.segment.local.upsert.merger.IncrementMerger;
import org.apache.pinot.segment.local.upsert.merger.OverwriteMerger;
import org.apache.pinot.segment.local.upsert.merger.PartialUpsertMerger;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.readers.GenericRow;


public class PartialUpsertHandler {
  private HashMap<String, PartialUpsertMerger> _mergers;
  private UpsertConfig.STRATEGY _defaultStrategy;

  /**
   * Initializes the partial upsert merger with upsert config. Different fields can have different merge strategies.
   *
   * @param globalUpsertStrategy can be derived into fields to merger map.
   * @param partialUpsertStrategies can be derived into fields to merger map.
   */
  public void init(UpsertConfig.STRATEGY globalUpsertStrategy,
      Map<String, UpsertConfig.STRATEGY> partialUpsertStrategies) {
    _mergers = new HashMap<>();
    _defaultStrategy = globalUpsertStrategy;

    for (String fieldName : partialUpsertStrategies.keySet()) {
      UpsertConfig.STRATEGY strategy = partialUpsertStrategies.get(fieldName);
      if (strategy != _defaultStrategy) {
        if (strategy == UpsertConfig.STRATEGY.IGNORE) {
          _mergers.put(fieldName, new IgnoreMerger());
        } else if (strategy == UpsertConfig.STRATEGY.INCREMENT) {
          _mergers.put(fieldName, new IncrementMerger());
        } else if (strategy == UpsertConfig.STRATEGY.OVERWRITE) {
          _mergers.put(fieldName, new OverwriteMerger());
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

    // init new row to avoid mergers conflict
    GenericRow row;
    if (_defaultStrategy == UpsertConfig.STRATEGY.IGNORE) {
      row = previousRecord;
    } else {
      row = newRecord;
    }

    // object can be null, handle null values in mergers
    for (String fieldName : _mergers.keySet()) {
      if (newRecord.isNullValue(fieldName)) {
        row.putDefaultNullValue(fieldName, previousRecord.getValue(fieldName));
      } else if (previousRecord.isNullValue(fieldName)) {
        row.putDefaultNullValue(fieldName, newRecord.getValue(fieldName));
      } else {
        Object newValue =
            _mergers.get(fieldName).merge(previousRecord.getValue(fieldName), newRecord.getValue(fieldName));
        row.putValue(fieldName, newValue);
      }
    }
    return row;
  }
}
