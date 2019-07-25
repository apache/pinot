package org.apache.pinot.tools.tuner.meta.manager.metadata.collector;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.tools.tuner.strategy.AbstractAccumulator;


public class ColStatsAccumulatorObj extends AbstractAccumulator {

  Map<String, String> accumulatedStats = new HashMap<>();
  Map<String, Map<String, String>> segmentStats = new HashMap<>();

  @Override
  public String toString() {
    return "ColStatsAccumulatorObj{" + "accumulatedStats=" + accumulatedStats + ", segmentStats=" + segmentStats + '}';
  }

  private String _segmentName;
  private String _cardinality;
  private String _totalDocs;
  private String _totalNumberOfEntries;
  private String _dataType;
  private String _columnType;
  private String _isSorted;
  private String _hasInvertedIndex;

  public ColStatsAccumulatorObj addSegmentName(String segmentName) {
    _segmentName = segmentName;
    return this;
  }

  public ColStatsAccumulatorObj addCardinality(String cardinality) {
    _cardinality = cardinality;
    return this;
  }

  public ColStatsAccumulatorObj addTotalDocs(String totalDocs) {
    _totalDocs = totalDocs;
    return this;
  }

  public ColStatsAccumulatorObj addTotalNumberOfEntries(String totalNumberOfEntries) {
    _totalNumberOfEntries = totalNumberOfEntries;
    return this;
  }

  public ColStatsAccumulatorObj addDataType(String dataType) {
    _dataType = dataType;
    return this;
  }

  public ColStatsAccumulatorObj addColumnType(String columnType) {
    _columnType = columnType;
    return this;
  }

  public ColStatsAccumulatorObj addIsSorted(String isSorted) {
    _isSorted = isSorted;
    return this;
  }

  public ColStatsAccumulatorObj addHasInvertedIndex(String hasInvertedIndex) {
    _hasInvertedIndex = hasInvertedIndex;
    return this;
  }

  public void merge() {
  }

  public void merge(ColStatsAccumulatorObj colStatsAccumulatorObj) {

  }

}
