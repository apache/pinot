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

import java.util.Map;
import org.apache.pinot.segment.local.segment.readers.LazyRow;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Merger previously persisted row with the new incoming row.
 * <p>
 * Implement this interface to define logic to merge rows. {@link LazyRow} provides abstraction row like abstraction
 * to read previously persisted row by lazily loading column values if needed. For automatic plugging of the
 * interface via {@link org.apache.pinot.segment.local.upsert.merger.columnar.PartialUpsertColumnMergerFactory}
 * implement {@link BasePartialUpsertMerger}
 */
public interface PartialUpsertMerger {

  /**
   * Merge previous row with new incoming row and persist the merged results per column in the provided
   * mergerResult map. {@link org.apache.pinot.segment.local.upsert.PartialUpsertHandler} ensures the primary key and
   * comparison columns are not modified, comparison columns are merged and only the latest non values are stored.
   * @param prevRecord
   * @param newRecord
   * @param mergerResult
   */
  void merge(LazyRow prevRecord, GenericRow newRecord, Map<String, Object> mergerResult);
}
