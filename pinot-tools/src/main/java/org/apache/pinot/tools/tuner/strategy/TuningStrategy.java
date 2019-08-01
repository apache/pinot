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
package org.apache.pinot.tools.tuner.strategy;

import java.util.Map;
import org.apache.pinot.tools.tuner.meta.manager.MetaManager;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.AbstractQueryStats;


/**
 * Recommendation strategy interface.
 */
public interface TuningStrategy {

  /**
   * Filter out irrelevant query stats to target a specific table or specific range of nESI
   * @param queryStats the stats extracted and parsed from InputIterator
   */
  boolean filter(AbstractQueryStats queryStats);

  /**
   * Accumulate the parsed queryStats to corresponding entry in MapperOut
   * @param queryStats input, the stats extracted and parsed from InputIterator
   * @param metaManager input, the metaManager where cardinality info can be get from
   * @param accumulatorOut output, map of /tableMame: String/columnName: String/AbstractMergerObj
   */
  void accumulate(AbstractQueryStats queryStats, MetaManager metaManager,
      Map<String, Map<String, AbstractAccumulator>> accumulatorOut);

  /**
   * merge two AbstractMergerObj with same /tableName/colName
   * @param abstractAccumulator input
   * @param abstractAccumulatorToMerge input
   */
  void merge(AbstractAccumulator abstractAccumulator, AbstractAccumulator abstractAccumulatorToMerge);
  //Merge two AbstractMergerObj from same /table/column

  /**
   * Generate a report for recommendation using tableResults:tableName/colName/AbstractMergerObj
   * @param tableResults input
   */
  void report(Map<String, Map<String, AbstractAccumulator>> tableResults);
  //print results
}
