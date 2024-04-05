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
 * Merger to merge previously persisted row with the new incoming row.
 * Custom implementation can be plugged by implementing this interface and add the class name to the upsert config.
 */
public interface PartialUpsertMerger {

  /**
   * Merges previous row with new incoming row and persists the merged results per column in the provided resultHolder.
   * Primary key and comparison columns should not be merged because their values are not allowed to be modified.
   */
  void merge(LazyRow previousRow, GenericRow newRow, Map<String, Object> resultHolder);
}
