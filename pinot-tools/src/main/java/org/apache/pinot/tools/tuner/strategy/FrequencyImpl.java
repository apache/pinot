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

import io.vavr.Tuple2;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.apache.commons.math.fraction.BigFraction;
import org.apache.pinot.tools.tuner.meta.manager.MetaManager;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.AbstractQueryStats;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.IndexSuggestQueryStatsImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A strategy of simply counting the number of appearances of each dimension and rank them.
 */
public class FrequencyImpl implements TuningStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(FrequencyImpl.class);

  private static final String NUM_QUERIES_COUNT = "PINOT_TUNER_COUNT*";

  public final static String DIMENSION_REGEX = "(?:(\\w+) ((?:NOT )?IN) (\\(.+?\\)))|(?:(\\w+) (=|<>|!=) (.+?)[ |$)])";

  public final static long DEFAULT_IN_FILTER_THRESHOLD = 0;
  public final static long DEFAULT_CARDINALITY_THRESHOLD = 1;
  public final static long DEFAULT_NUM_QUERIES_THRESHOLD = 0;
  public static final int MATCHER_GROUP_DIMENSION_IN = 1;
  public static final int MATCHER_GROUP_DIMENSION_COMP = 4;

  public final static Pattern _dimensionPattern = Pattern.compile(DIMENSION_REGEX);
  private HashSet<String> _tableNamesWithoutType;
  private long _numEntriesScannedThreshold;
  private long _cardinalityThreshold;
  private long _numQueriesThreshold;
  private boolean _skipTableCheck;

  private FrequencyImpl(Builder builder) {
    _tableNamesWithoutType = builder._tableNamesWithoutType;
    _numEntriesScannedThreshold = builder._numEntriesScannedThreshold;
    _cardinalityThreshold = builder._cardinalityThreshold;
    _numQueriesThreshold = builder._numQueriesThreshold;
    _skipTableCheck = (_tableNamesWithoutType == null) || _tableNamesWithoutType.isEmpty();
  }

  public static final class Builder {
    private HashSet<String> _tableNamesWithoutType = new HashSet<>();
    private long _numEntriesScannedThreshold = DEFAULT_IN_FILTER_THRESHOLD;
    private long _cardinalityThreshold = DEFAULT_CARDINALITY_THRESHOLD;
    private long _numQueriesThreshold = DEFAULT_NUM_QUERIES_THRESHOLD;

    public Builder() {
    }

    @Nonnull
    public FrequencyImpl build() {
      return new FrequencyImpl(this);
    }

    /**
     * set the tables to work on, other tables will be filtered out
     * @param val set of table names without type
     */
    @Nonnull
    public Builder setTableNamesWithoutType(@Nonnull HashSet<String> val) {
      _tableNamesWithoutType = val;
      return this;
    }

    /**
     * set the threshold for _numEntriesScannedInFilter, the queries with _numEntriesScannedInFilter below this will be filtered out
     * @param val threshold for _numEntriesScannedInFilter, default to 0
     */
    @Nonnull
    public Builder setNumEntriesScannedThreshold(long val) {
      _numEntriesScannedThreshold = val;
      return this;
    }

    /**
     * set the cardinality threshold, column with cardinality below this will be ignored,
     * setting a high value will force the system to ignore low card columns
     * @param val cardinality threshold, default to 1
     */
    @Nonnull
    public Builder setCardinalityThreshold(long val) {
      _cardinalityThreshold = val;
      return this;
    }

    /**
     * set the minimum number of records scanned to give a recommendation
     * @param val minimum number of records scanned to give a recommendation, default to 0
     */
    @Nonnull
    public Builder setNumQueriesThreshold(long val) {
      _numQueriesThreshold = val;
      return this;
    }
  }

  @Override
  public boolean filter(AbstractQueryStats queryStats) {
    IndexSuggestQueryStatsImpl indexSuggestQueryStatsImpl = (IndexSuggestQueryStatsImpl) queryStats;
    long numEntriesScannedInFilter = Long.parseLong(indexSuggestQueryStatsImpl.getNumEntriesScannedInFilter());
    return (_skipTableCheck || _tableNamesWithoutType.contains(
        indexSuggestQueryStatsImpl.getTableNameWithoutType())) && (numEntriesScannedInFilter > _numEntriesScannedThreshold);
  }

  @Override
  public void accumulate(AbstractQueryStats queryStats, MetaManager metaManager, Map<String, Map<String, AbstractAccumulator>> accumulatorOut) {

    IndexSuggestQueryStatsImpl indexSuggestQueryStatsImpl = (IndexSuggestQueryStatsImpl) queryStats;
    String tableNameWithoutType = indexSuggestQueryStatsImpl.getTableNameWithoutType();
    String numEntriesScannedInFilter = indexSuggestQueryStatsImpl.getNumEntriesScannedInFilter();
    String query = indexSuggestQueryStatsImpl.getQuery();
    LOGGER.debug("Accumulator: scoring query {}", query);
    HashSet<String> counted = new HashSet<>();

    AbstractAccumulator.putAccumulatorToMapIfAbsent(accumulatorOut, tableNameWithoutType, NUM_QUERIES_COUNT,
        new FrequencyAccumulator()).increaseCount();

    Matcher matcher = _dimensionPattern.matcher(query);
    while (matcher.find()) {
      if (matcher.group(MATCHER_GROUP_DIMENSION_IN) != null) {
        counted.add(matcher.group(MATCHER_GROUP_DIMENSION_IN));
      } else if (matcher.group(MATCHER_GROUP_DIMENSION_COMP) != null) {
        counted.add(matcher.group(MATCHER_GROUP_DIMENSION_COMP));
      }
    }

    counted.stream()
        .filter(colName -> metaManager.getColumnSelectivity(tableNameWithoutType, colName).compareTo(new BigFraction(_cardinalityThreshold)) > 0)
        .forEach(colName -> {
          ((FrequencyAccumulator) AbstractAccumulator.putAccumulatorToMapIfAbsent(accumulatorOut, tableNameWithoutType,
              colName, new FrequencyAccumulator())).incrementFrequency();
        });
  }

  @Override
  public void merge(AbstractAccumulator p1, AbstractAccumulator p2) {
    ((FrequencyAccumulator) p1).merge((FrequencyAccumulator) p2);
  }

  /**
   * Generate a report for recommendation using tableResults:tableName/colName/AbstractMergerObj
   * @param tableResults input
   */
  public void report(Map<String, Map<String, AbstractAccumulator>> tableResults) {
    tableResults.forEach((table, map) -> {
      reportTable(table, map);
    });
  }

  private void reportTable(String tableNameWithoutType, Map<String, AbstractAccumulator> columnStats) {
    String reportOut = "\n**********************Report For Table: " + tableNameWithoutType + "**********************\n";
    long totalCount = columnStats.remove(NUM_QUERIES_COUNT).getCount();
    if (totalCount < _numQueriesThreshold) {
      reportOut += "No enough data accumulated for this table!\n";
      LOGGER.info(reportOut);
      return;
    }

    reportOut += MessageFormat.format("\nTotal lines accumulated: {0}\n\n", totalCount);
    List<Tuple2<String, Long>> sortedPure = new ArrayList<>();
    columnStats.forEach(
        (colName, score) -> sortedPure.add(new Tuple2<>(colName, ((FrequencyAccumulator) score).getFrequency())));
    sortedPure.sort((p1, p2) -> (p2._2().compareTo(p1._2())));
    for (Tuple2<String, Long> tuple2 : sortedPure) {
      reportOut += "Dimension: " + tuple2._1() + "  " + tuple2._2().toString() + "\n";
    }
    LOGGER.info(reportOut);
  }
}

