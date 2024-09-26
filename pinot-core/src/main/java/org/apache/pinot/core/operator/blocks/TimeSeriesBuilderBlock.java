package org.apache.pinot.core.operator.blocks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.combine.merger.TimeSeriesAggResultsBlockMerger;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.BaseTimeSeriesBuilder;
import org.apache.pinot.tsdb.spi.series.TimeSeries;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;


/**
 * Block used by the {@link TimeSeriesAggResultsBlockMerger}.
 */
public class TimeSeriesBuilderBlock {
  private final TimeBuckets _timeBuckets;
  private final Map<Long, BaseTimeSeriesBuilder> _seriesBuilderMap;

  public TimeSeriesBuilderBlock(TimeBuckets timeBuckets, Map<Long, BaseTimeSeriesBuilder> seriesBuilderMap) {
    _timeBuckets = timeBuckets;
    _seriesBuilderMap = seriesBuilderMap;
  }

  public TimeBuckets getTimeBuckets() {
    return _timeBuckets;
  }

  public Map<Long, BaseTimeSeriesBuilder> getSeriesBuilderMap() {
    return _seriesBuilderMap;
  }

  public TimeSeriesBlock build() {
    Map<Long, List<TimeSeries>> seriesMap = new HashMap<>();
    for (var entry : _seriesBuilderMap.entrySet()) {
      List<TimeSeries> result =  new ArrayList<>(1);
      result.add(entry.getValue().build());
      seriesMap.put(entry.getKey(), result);
    }
    return new TimeSeriesBlock(_timeBuckets, seriesMap);
  }
}
