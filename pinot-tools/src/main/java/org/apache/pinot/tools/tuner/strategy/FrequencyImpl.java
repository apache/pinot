package org.apache.pinot.tools.tuner.strategy;

import io.vavr.Tuple2;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.apache.commons.math.fraction.BigFraction;
import org.apache.pinot.tools.tuner.meta.manager.MetaDataProperties;
import org.apache.pinot.tools.tuner.query.src.BasicQueryStats;
import org.apache.pinot.tools.tuner.query.src.IndexSuggestQueryStatsImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FrequencyImpl implements BasicStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(FrequencyImpl.class);

  public final static String DIMENSION_REGEX="(?:(\\w+) ((?:NOT )?IN) (\\(.+?\\)))|(?:(\\w+) (=|<>|!=) (.+?)[ |$)])";
  public final static long NO_IN_FILTER_THRESHOLD = 0;
  public final static long CARD_THRESHOLD_ONE = 1;

  public final static Pattern _dimensionPattern=Pattern.compile(DIMENSION_REGEX);
  private HashSet<String> _tableNamesWorkonWithoutType;
  private long _numEntriesScannedThreshold;
  private long _cardinalityThreshold;

  private FrequencyImpl(Builder builder) {
    _tableNamesWorkonWithoutType = builder._tableNamesWorkonWithoutType;
    _numEntriesScannedThreshold = builder._numEntriesScannedThreshold;
    _cardinalityThreshold = builder._cardinalityThreshold;
  }

  public static final class Builder {
    private HashSet<String> _tableNamesWorkonWithoutType = new HashSet<>();
    private long _numEntriesScannedThreshold = NO_IN_FILTER_THRESHOLD;
    private long _cardinalityThreshold=CARD_THRESHOLD_ONE;

    public Builder() {
    }

    @Nonnull
    public FrequencyImpl build() {
      return new FrequencyImpl(this);
    }

    @Nonnull
    public Builder _tableNamesWorkonWithoutType(@Nonnull HashSet<String> val) {
      _tableNamesWorkonWithoutType = val;
      return this;
    }

    @Nonnull
    public Builder _numEntriesScannedThreshold(long val) {
      _numEntriesScannedThreshold = val;
      return this;
    }

    @Nonnull
    public Builder _cardinalityThreshold(long val) {
      _cardinalityThreshold = val;
      return this;
    }

    @Nonnull
    public Builder _tableNamesWorkonWithoutType(@Nonnull List<String> val) {
      _tableNamesWorkonWithoutType.addAll(val);
      return this;
    }
  }

  @Override
  public boolean filter(BasicQueryStats queryStats) {
    IndexSuggestQueryStatsImpl indexSuggestQueryStatsImpl = (IndexSuggestQueryStatsImpl) queryStats;
    long numEntriesScannedInFilter = Long.parseLong(indexSuggestQueryStatsImpl.getNumEntriesScannedInFilter());
    return (_tableNamesWorkonWithoutType.isEmpty() || _tableNamesWorkonWithoutType
        .contains(indexSuggestQueryStatsImpl.getTableNameWithoutType())) && (numEntriesScannedInFilter
        >= _numEntriesScannedThreshold);
  }

  @Override
  public void accumulator(BasicQueryStats queryStats, MetaDataProperties metaDataProperties,
      Map<String, Map<String, BasicMergerObj>> AccumulatorOut) {

    IndexSuggestQueryStatsImpl indexSuggestQueryStatsImpl = (IndexSuggestQueryStatsImpl) queryStats;
    String tableNameWithoutType = indexSuggestQueryStatsImpl.getTableNameWithoutType();
    String numEntriesScannedInFilter = indexSuggestQueryStatsImpl.getNumEntriesScannedInFilter();
    String query = indexSuggestQueryStatsImpl.getQuery();
    LOGGER.debug("Accumulator: scoring query {}", query);
    HashSet<String> counted=new HashSet<>();
    if (Long.parseLong(numEntriesScannedInFilter) == 0) return; //Early return if the query is not scanning in filter

    Matcher matcher=_dimensionPattern.matcher(query);
    while(matcher.find()){
      if (matcher.group(1)!=null){
        counted.add(matcher.group(1));
      }
      else if (matcher.group(4)!=null){
        counted.add(matcher.group(4));
      }
      else{}
    }
    counted.stream().filter(colName->metaDataProperties.getAverageCardinality(tableNameWithoutType,colName).compareTo(new BigFraction(_cardinalityThreshold))>0)
        .forEach(colName->{
          AccumulatorOut.putIfAbsent(tableNameWithoutType, new HashMap<>());
          AccumulatorOut.get(tableNameWithoutType).putIfAbsent(colName, new FrequencyBasicMergerObj());
          ((FrequencyBasicMergerObj)AccumulatorOut.get(tableNameWithoutType).get(colName)).merge(1);
        });
  }

  @Override
  public void merger(BasicMergerObj p1, BasicMergerObj p2) {
    ((FrequencyBasicMergerObj) p1).merge((FrequencyBasicMergerObj) p2);
  }

  @Override
  public void reporter(String tableNameWithoutType, Map<String, BasicMergerObj> mergedOut) {
    String tableName = "\n**********************Report For Table: " + tableNameWithoutType + "**********************\n";
    String mergerOut = "";
    List<Tuple2<String, Long>> sortedPure = new ArrayList<>();
    mergedOut
        .forEach((colName, score) -> sortedPure.add(new Tuple2<>(colName, ((FrequencyBasicMergerObj) score).getPureScore())));
    sortedPure.sort((p1,p2)->(p2._2().compareTo(p1._2())));
    for (Tuple2<String, Long> tuple2 : sortedPure) {
      mergerOut += "Dimension: " + tuple2._1()+ "  " + tuple2._2().toString() + "\n";
    }
    LOGGER.info(tableName + mergerOut);
  }
}

