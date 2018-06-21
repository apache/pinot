package com.linkedin.thirdeye.detection.algorithm;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import com.linkedin.thirdeye.detection.StaticDetectionPipeline;
import com.linkedin.thirdeye.detection.StaticDetectionPipelineData;
import com.linkedin.thirdeye.detection.StaticDetectionPipelineModel;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import com.linkedin.thirdeye.rootcause.timeseries.Baseline;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineAggregate;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineAggregateType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.collections.MapUtils;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


/**
 * This algorithm compares the subsequence (default length is 3 data point) of current time series and baseline time series and
 * calculate the euclidean distance between them. If the kth nearest distance is above a certain threshold, it marks it as an anomaly.
 */
public class WindowBasedNearestNeighborAlgorithm extends StaticDetectionPipeline {
  private static final String PROP_METRIC_URN = "metricUrn";

  private static final String PROP_AGGREGATION_DEFAULT = "MEDIAN";

  private static final String PROP_TIMEZONE = "timezone";
  private static final String PROP_TIMEZONE_DEFAULT = "UTC";

  private static final String PROP_WINDOW_SIZE = "windowSize";
  private static final Logger LOG = LoggerFactory.getLogger(WindowBasedNearestNeighborAlgorithm.class);

  private MetricSlice slice;
  private Baseline baseline;
  private final DateTimeZone timezone;
  private int windowSize;
  private Double threshold;

  public WindowBasedNearestNeighborAlgorithm(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    super(provider, config, startTime, endTime);

    Preconditions.checkArgument(config.getProperties().containsKey(PROP_METRIC_URN));

    String metricUrn = MapUtils.getString(config.getProperties(), PROP_METRIC_URN);
    MetricEntity me = MetricEntity.fromURN(metricUrn, 1.0);
    this.slice = MetricSlice.from(me.getId(), this.startTime, this.endTime, me.getFilters());

    // window size in terms of number of data points
    this.windowSize = MapUtils.getInteger(config.getProperties(), PROP_WINDOW_SIZE, 3);
    this.threshold = MapUtils.getDouble(config.getProperties(), "threshold", null);

    this.timezone = DateTimeZone.forID(MapUtils.getString(config.getProperties(), PROP_TIMEZONE, PROP_TIMEZONE_DEFAULT));
    int baselineWeeks = MapUtils.getIntValue(config.getProperties(), "baselineWeeks", 4);
    BaselineAggregateType baselineType = BaselineAggregateType.valueOf(MapUtils.getString(config.getProperties(), "baselineType", PROP_AGGREGATION_DEFAULT));
    if (baselineWeeks <= 0) {
      this.baseline = null;
    } else {
      this.baseline = BaselineAggregate.fromWeekOverWeek(baselineType, baselineWeeks, 1, this.timezone);
    }

  }

  @Override
  public StaticDetectionPipelineModel getModel() {
    List<MetricSlice> slices = new ArrayList<>(this.baseline.scatter(this.slice));
    slices.add(this.slice);

    return new StaticDetectionPipelineModel()
        .withTimeseriesSlices(slices);
  }

  @Override
  public DetectionPipelineResult run(StaticDetectionPipelineData data) {
    DataFrame dfCurr = data.getTimeseries().get(this.slice);
    DataFrame dfBase = this.baseline.gather(this.slice, data.getTimeseries());

    if (this.threshold == null) {
      this.threshold =  0.1 * dfBase.getDoubles(COL_VALUE).std().getDouble(0);
    }
    Set<DataFrame> currentWindowSlicedDataFrames = getWindowSlicedDataFrames(dfCurr);
    Set<DataFrame> baselineWindowSlicedDataFrames = getWindowSlicedDataFrames(dfBase);

    List<MergedAnomalyResultDTO> anomalies = kNNThreshHold(currentWindowSlicedDataFrames, baselineWindowSlicedDataFrames,  this.windowSize * this.threshold);

    return new DetectionPipelineResult(anomalies);
  }

  private List<MergedAnomalyResultDTO> kNNThreshHold(Set<DataFrame> currentDfs, Set<DataFrame> baselineDfs, double threshold) {
    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    for (DataFrame currentDf : currentDfs) {
      List<Double> distances = new ArrayList<>();
      for (DataFrame baselineDf : baselineDfs) {
        distances.add(euclideanDistanceSquare(currentDf, baselineDf));
      }
      Collections.sort(distances);
      if (distances.get((int) Math.sqrt(distances.size())) > threshold * threshold) {
        LongSeries timeStamps = currentDf.getLongs(COL_TIME);
        anomalies.add(makeAnomaly(this.slice.withStart(timeStamps.get(0)).withEnd(timeStamps.get(timeStamps.size() - 1))));
      }
    }
    return anomalies;
  }

  private double euclideanDistanceSquare(DataFrame df1, DataFrame df2){
    DoubleSeries df1Values = df1.getDoubles(COL_VALUE);
    DoubleSeries df2Values = df2.getDoubles(COL_VALUE);
    if (df1Values.size() != df2Values.size()) {
      return 0;
    }
    DoubleSeries values = df1Values.subtract(df2Values);
    return values.multiply(values).sum().getDouble(0);
  }

  private Set<DataFrame> getWindowSlicedDataFrames(DataFrame df) {
    LongSeries currentTimestamps = df.getLongs(COL_TIME).filter(df.getLongs(COL_TIME).between(this.startTime, this.endTime)).dropNull();
    Set<DataFrame> windowSliceDataFrames = new HashSet<>();
    for (int i = 0; i < currentTimestamps.size() - this.windowSize; i++){
      long ts = currentTimestamps.get(i);
        DataFrame windowSliceDf =
            df.filter(df.getLongs(COL_TIME).between(ts, currentTimestamps.get(i + this.windowSize))).dropNull(COL_TIME);
        windowSliceDataFrames.add(windowSliceDf);

    }
    return windowSliceDataFrames;
  }
}
