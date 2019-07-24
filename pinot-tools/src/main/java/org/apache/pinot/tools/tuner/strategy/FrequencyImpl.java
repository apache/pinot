package org.apache.pinot.tools.tuner.strategy;

import io.vavr.Tuple2;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.apache.commons.math.fraction.BigFraction;
import org.apache.pinot.tools.tuner.meta.manager.MetaManager;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.AbstractQueryStats;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.IndexSuggestQueryStatsImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FrequencyImpl implements Strategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(FrequencyImpl.class);

  public final static String DIMENSION_REGEX = "(?:(\\w+) ((?:NOT )?IN) (\\(.+?\\)))|(?:(\\w+) (=|<>|!=) (.+?)[ |$)])";
  public final static long NO_IN_FILTER_THRESHOLD = 0;
  public final static long CARD_THRESHOLD_ONE = 1;

  public final static long NO_PROCESSED_THRESH = 0;

  public final static Pattern _dimensionPattern = Pattern.compile(DIMENSION_REGEX);
  private HashSet<String> _tableNamesWorkonWithoutType;
  private long _numEntriesScannedThreshold;
  private long _cardinalityThreshold;
  private long _numProcessedThreshold;

  private FrequencyImpl(Builder builder) {
    _tableNamesWorkonWithoutType = builder._tableNamesWorkonWithoutType;
    _numEntriesScannedThreshold = builder._numEntriesScannedThreshold;
    _cardinalityThreshold = builder._cardinalityThreshold;
    _numProcessedThreshold = builder._numProcessedThreshold;
  }

  public static final class Builder {
    private HashSet<String> _tableNamesWorkonWithoutType = new HashSet<>();
    private long _numEntriesScannedThreshold = NO_IN_FILTER_THRESHOLD;
    private long _cardinalityThreshold = CARD_THRESHOLD_ONE;
    private long _numProcessedThreshold = NO_PROCESSED_THRESH;

    public Builder() {
    }

    @Nonnull
    public FrequencyImpl build() {
      return new FrequencyImpl(this);
    }

    /**
     * set the tables to work on, other tables will be filtered out
     * @param val set of table names without type
     * @return
     */
    @Nonnull
    public Builder setTableNamesWorkonWithoutType(@Nonnull HashSet<String> val) {
      _tableNamesWorkonWithoutType = val;
      return this;
    }

    /**
     * set the threshold for _numEntriesScannedInFilter, the queries with _numEntriesScannedInFilter below this will be filtered out
     * @param val
     * @return
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
     * @return
     */
    @Nonnull
    public Builder setCardinalityThreshold(long val) {
      _cardinalityThreshold = val;
      return this;
    }

    /**
     * set the minimum number of records scanned to give a recommendation
     * @param val minimum number of records scanned to give a recommendation, default to 0
     * @return
     */
    @Nonnull
    public Builder setNumProcessedThreshold(long val) {
      _numProcessedThreshold = val;
      return this;
    }

    /**
     * set the tables to work on, other tables will be filtered out
     * @param val list of table names without type
     * @return
     */
    @Nonnull
    public Builder setTableNamesWorkonWithoutType(@Nonnull List<String> val) {
      _tableNamesWorkonWithoutType.addAll(val);
      return this;
    }
  }

  @Override
  public boolean filter(AbstractQueryStats queryStats) {
    IndexSuggestQueryStatsImpl indexSuggestQueryStatsImpl = (IndexSuggestQueryStatsImpl) queryStats;
    long numEntriesScannedInFilter = Long.parseLong(indexSuggestQueryStatsImpl.getNumEntriesScannedInFilter());
    return (_tableNamesWorkonWithoutType.isEmpty() || _tableNamesWorkonWithoutType
        .contains(indexSuggestQueryStatsImpl.getTableNameWithoutType())) && (numEntriesScannedInFilter
        >= _numEntriesScannedThreshold);
  }

  @Override
  public void accumulate(AbstractQueryStats queryStats, MetaManager metaManager,
      Map<String, Map<String, AbstractAccumulator>> AccumulatorOut) {

    IndexSuggestQueryStatsImpl indexSuggestQueryStatsImpl = (IndexSuggestQueryStatsImpl) queryStats;
    String tableNameWithoutType = indexSuggestQueryStatsImpl.getTableNameWithoutType();
    String numEntriesScannedInFilter = indexSuggestQueryStatsImpl.getNumEntriesScannedInFilter();
    String query = indexSuggestQueryStatsImpl.getQuery();
    LOGGER.debug("Accumulator: scoring query {}", query);
    HashSet<String> counted = new HashSet<>();

    if (Long.parseLong(numEntriesScannedInFilter) == 0) {
      return; //Early return if the query is not scanning in filter
    }

    Matcher matcher = _dimensionPattern.matcher(query);
    while (matcher.find()) {
      if (matcher.group(1) != null) {
        counted.add(matcher.group(1));
      } else if (matcher.group(4) != null) {
        counted.add(matcher.group(4));
      } else {
      }
    }

    counted.stream().filter(colName -> metaManager.getColumnSelectivity(tableNameWithoutType, colName)
        .compareTo(new BigFraction(_cardinalityThreshold)) > 0).forEach(colName -> {
      AccumulatorOut.putIfAbsent(tableNameWithoutType, new HashMap<>());
      AccumulatorOut.get(tableNameWithoutType).putIfAbsent(colName, new FrequencyAccumulator());
      ((FrequencyAccumulator) AccumulatorOut.get(tableNameWithoutType).get(colName)).merge(1);
    });
  }

  @Override
  public void merge(AbstractAccumulator p1, AbstractAccumulator p2) {
    ((FrequencyAccumulator) p1).merge((FrequencyAccumulator) p2);
  }

  @Override
  public void report(String tableNameWithoutType, Map<String, AbstractAccumulator> mergedOut) {
    AtomicLong totalCount = new AtomicLong(0);
    mergedOut.forEach((k, v) -> {
      totalCount.addAndGet(v.getCount());
    });
    if (totalCount.longValue() < _numProcessedThreshold) {
      return;
    }

    String reportOut = "\n**********************Report For Table: " + tableNameWithoutType + "**********************\n";
    List<Tuple2<String, Long>> sortedPure = new ArrayList<>();
    mergedOut.forEach(
        (colName, score) -> sortedPure.add(new Tuple2<>(colName, ((FrequencyAccumulator) score).getPureScore())));
    sortedPure.sort((p1, p2) -> (p2._2().compareTo(p1._2())));
    for (Tuple2<String, Long> tuple2 : sortedPure) {
      reportOut += "Dimension: " + tuple2._1() + "  " + tuple2._2().toString() + "\n";
    }
    LOGGER.info(reportOut);
  }
}

