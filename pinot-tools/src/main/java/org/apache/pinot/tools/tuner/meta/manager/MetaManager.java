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
package org.apache.pinot.tools.tuner.meta.manager;

import org.apache.commons.math.fraction.BigFraction;


/**
 * Load and access fields of metadata.properties
 */
public interface MetaManager {
  String WEIGHTED_SUM_CARDINALITY = "w_cardinality";
  String SUM_DOCS = "n_totalDocs";
  String SUM_SEGMENTS_HAS_INVERTED_INDEX = "n_hasInvertedIndex";
  String SUM_SEGMENTS_SORTED = "n_isSorted";
  String SUM_SEGMENTS_COUNT = "n_segmentCount";
  String SUM_TOTAL_ENTRIES = "n_totalNumberOfEntries";

  /**
   * Get aggregated (sum and weighted sum) of metadata
   * @param tableNameWithoutType
   * @param columnName
   * @param fieldName one of {WEIGHTED_SUM_CARDINALITY, SUM_DOCS, NUM_SEGMENTS_HAS_INVERTED_INDEX, NUM_SEGMENTS_SORTED, NUM_SEGMENTS_COUNT}
   * @return
   */
  String getColField(String tableNameWithoutType, String columnName, String fieldName);

  /**
   * Getter to certain fields for a specific column, for forward compatibility precise inverted index prediction.
   * @param tableNameWithoutType
   * @param columnName
   * @param segmentName
   * @param fieldName
   * @return
   */
  String getSegmentField(String tableNameWithoutType, String columnName, String segmentName, String fieldName); //get metadata of individual segment

  /**
   * Get the selectivity calculated by weighted average of cardinality.
   * @param tableNameWithoutType
   * @param columnName
   * @return Selectivity, a BigFraction.
   */
  BigFraction getColumnSelectivity(String tableNameWithoutType, String columnName);

  /**
   * Get the average num of entries per doc.
   * @param tableNameWithoutType
   * @param columnName
   * @return
   */
  BigFraction getAverageNumEntriesPerDoc(String tableNameWithoutType, String columnName);

  boolean hasInvertedIndex(String tableNameWithoutType, String columnName);
}
