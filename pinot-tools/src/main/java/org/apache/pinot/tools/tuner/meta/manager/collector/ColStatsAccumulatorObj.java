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
package org.apache.pinot.tools.tuner.meta.manager.collector;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.tools.tuner.meta.manager.MetaManager;
import org.apache.pinot.tools.tuner.strategy.AbstractAccumulator;


/**
 * Accumulator for relevant fields in metadata.properties and index_map
 */
public class ColStatsAccumulatorObj extends AbstractAccumulator {

  private Map<String, BigInteger> _accumulatedStats = new HashMap<>();
  private Map<String, Map<String, String>> _segmentStats = new HashMap<>();

  private static final String CARDINALITY = "cardinality";
  private static final String TOTAL_DOCS = "totalDocs";
  private static final String TOTAL_NUMBER_OF_ENTRIES = "totalNumberOfEntries";
  private static final String IS_SORTED = "isSorted";
  private static final String INVERTED_INDEX_SIZE = "invertedIndexSize";

  @Override
  public String toString() {
    return "ColStatsAccumulatorObj{" + "accumulatedStats=" + _accumulatedStats + ", segmentStats=" + _segmentStats
        + '}';
  }

  private String _segmentName;
  private String _cardinality;
  private String _totalDocs;
  private String _totalNumberOfEntries;
  private String _isSorted;
  private String _invertedIndexSize;

  Map<String, BigInteger> getAccumulatedStats() {
    return _accumulatedStats;
  }

  Map<String, Map<String, String>> getSegmentStats() {
    return _segmentStats;
  }

  ColStatsAccumulatorObj addInvertedIndexSize(String invertedIndexSize) {
    _invertedIndexSize = invertedIndexSize;
    return this;
  }

  ColStatsAccumulatorObj addSegmentName(String segmentName) {
    _segmentName = segmentName;
    return this;
  }

  ColStatsAccumulatorObj addCardinality(String cardinality) {
    _cardinality = cardinality;
    return this;
  }

  ColStatsAccumulatorObj addTotalDocs(String totalDocs) {
    _totalDocs = totalDocs;
    return this;
  }

  ColStatsAccumulatorObj addTotalNumberOfEntries(String totalNumberOfEntries) {
    _totalNumberOfEntries = totalNumberOfEntries;
    return this;
  }

  ColStatsAccumulatorObj addIsSorted(String isSorted) {
    _isSorted = isSorted;
    return this;
  }

  public void merge() {
    _accumulatedStats.put(MetaManager.WEIGHTED_SUM_CARDINALITY,
        _accumulatedStats.getOrDefault(MetaManager.WEIGHTED_SUM_CARDINALITY, BigInteger.ZERO)
            .add(new BigInteger(_cardinality).multiply(new BigInteger(_totalDocs))));

    _accumulatedStats.put(MetaManager.SUM_SEGMENTS_COUNT,
        _accumulatedStats.getOrDefault(MetaManager.SUM_SEGMENTS_COUNT, BigInteger.ZERO).add(BigInteger.ONE));

    _accumulatedStats.put(MetaManager.SUM_SEGMENTS_HAS_INVERTED_INDEX,
        _accumulatedStats.getOrDefault(MetaManager.SUM_SEGMENTS_HAS_INVERTED_INDEX, BigInteger.ZERO)
            .add(Integer.parseInt(_invertedIndexSize) > 0 ? BigInteger.ONE : BigInteger.ZERO));

    _accumulatedStats.put(MetaManager.SUM_SEGMENTS_SORTED,
        _accumulatedStats.getOrDefault(MetaManager.SUM_SEGMENTS_SORTED, BigInteger.ZERO)
            .add(_isSorted.trim().toLowerCase().equals("true") ? BigInteger.ONE : BigInteger.ZERO));

    _accumulatedStats.put(MetaManager.SUM_TOTAL_ENTRIES,
        _accumulatedStats.getOrDefault(MetaManager.SUM_TOTAL_ENTRIES, BigInteger.ZERO)
            .add(new BigInteger(_totalNumberOfEntries)));

    _accumulatedStats.put(MetaManager.SUM_DOCS,
        _accumulatedStats.getOrDefault(MetaManager.SUM_DOCS, BigInteger.ZERO).add(new BigInteger(_totalDocs)));

    HashMap<String, String> segmentMeta = new HashMap<>();
    segmentMeta.put(CARDINALITY, _cardinality);
    segmentMeta.put(TOTAL_DOCS, _totalDocs);
    segmentMeta.put(TOTAL_NUMBER_OF_ENTRIES, _totalNumberOfEntries);
    segmentMeta.put(IS_SORTED, _isSorted);
    segmentMeta.put(INVERTED_INDEX_SIZE, _invertedIndexSize);
    _segmentStats.put(_segmentName, segmentMeta);
  }

  public void merge(ColStatsAccumulatorObj colStatsAccumulatorObj) {
    _accumulatedStats.put(MetaManager.WEIGHTED_SUM_CARDINALITY,
        this._accumulatedStats.getOrDefault(MetaManager.WEIGHTED_SUM_CARDINALITY, BigInteger.ZERO)
            .add(colStatsAccumulatorObj._accumulatedStats.getOrDefault(MetaManager.WEIGHTED_SUM_CARDINALITY,
                BigInteger.ZERO)));

    _accumulatedStats.put(MetaManager.SUM_SEGMENTS_COUNT,
        this._accumulatedStats.getOrDefault(MetaManager.SUM_SEGMENTS_COUNT, BigInteger.ZERO)
            .add(colStatsAccumulatorObj._accumulatedStats.getOrDefault(MetaManager.SUM_SEGMENTS_COUNT,
                BigInteger.ZERO)));

    _accumulatedStats.put(MetaManager.SUM_SEGMENTS_HAS_INVERTED_INDEX,
        this._accumulatedStats.getOrDefault(MetaManager.SUM_SEGMENTS_HAS_INVERTED_INDEX, BigInteger.ZERO)
            .add(colStatsAccumulatorObj._accumulatedStats.getOrDefault(MetaManager.SUM_SEGMENTS_HAS_INVERTED_INDEX,
            BigInteger.ZERO)));

    _accumulatedStats.put(MetaManager.SUM_SEGMENTS_SORTED,
        this._accumulatedStats.getOrDefault(MetaManager.SUM_SEGMENTS_SORTED, BigInteger.ZERO)
            .add(colStatsAccumulatorObj._accumulatedStats.getOrDefault(MetaManager.SUM_SEGMENTS_SORTED,
                BigInteger.ZERO)));

    _accumulatedStats.put(MetaManager.SUM_TOTAL_ENTRIES,
        this._accumulatedStats.getOrDefault(MetaManager.SUM_TOTAL_ENTRIES, BigInteger.ZERO)
            .add(
                colStatsAccumulatorObj._accumulatedStats.getOrDefault(MetaManager.SUM_TOTAL_ENTRIES, BigInteger.ZERO)));

    _accumulatedStats.put(MetaManager.SUM_DOCS,
        this._accumulatedStats.getOrDefault(MetaManager.SUM_DOCS, BigInteger.ZERO)
            .add(colStatsAccumulatorObj._accumulatedStats.getOrDefault(MetaManager.SUM_DOCS, BigInteger.ZERO)));

    this._segmentStats.putAll(colStatsAccumulatorObj._segmentStats);
  }
}
