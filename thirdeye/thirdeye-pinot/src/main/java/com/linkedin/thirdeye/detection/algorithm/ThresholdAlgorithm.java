package com.linkedin.thirdeye.detection.algorithm;

import com.linkedin.thirdeye.dataframe.BooleanSeries;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import com.linkedin.thirdeye.detection.StaticDetectionPipeline;
import com.linkedin.thirdeye.detection.StaticDetectionPipelineData;
import com.linkedin.thirdeye.detection.StaticDetectionPipelineModel;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.collections.MapUtils;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


public class ThresholdAlgorithm extends StaticDetectionPipeline {
  private final String COL_TOO_HIGH = "tooHigh";
  private final String COL_TOO_LOW = "tooLow";
  private final String COL_ANOMALY = "anomaly";

  private final String PROP_METRIC_URN = "metricUrn";

  private final String PROP_MIN = "min";
  private final double PROP_MIN_DEFAULT = Double.NaN;

  private final String PROP_MAX = "max";
  private final double PROP_MAX_DEFAULT = Double.NaN;

  private final double min;
  private final double max;
  private final MetricSlice slice;


  public ThresholdAlgorithm(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    super(provider, config, startTime, endTime);
    this.min = MapUtils.getDoubleValue(config.getProperties(), PROP_MIN, PROP_MIN_DEFAULT);
    this.max = MapUtils.getDoubleValue(config.getProperties(), PROP_MAX, PROP_MAX_DEFAULT);

    String metricUrn = MapUtils.getString(config.getProperties(), PROP_METRIC_URN);
    MetricEntity me = MetricEntity.fromURN(metricUrn, 1.0);
    this.slice = MetricSlice.from(me.getId(), this.startTime, this.endTime, me.getFilters());
  }

  @Override
  public StaticDetectionPipelineModel getModel() {
    return new StaticDetectionPipelineModel()
        .withTimeseriesSlices(Collections.singletonList(this.slice));
  }

  @Override
  public DetectionPipelineResult run(StaticDetectionPipelineData data) {
    DataFrame df = data.getTimeseries().get(this.slice);

    df.addSeries(COL_TOO_HIGH, BooleanSeries.fillValues(df.size(), false));
    df.addSeries(COL_TOO_LOW, BooleanSeries.fillValues(df.size(), false));

    if (!Double.isNaN(this.max)) {
      df.mapInPlace(new Series.DoubleConditional() {
        @Override
        public boolean apply(double... values) {
          return values[0] > ThresholdAlgorithm.this.max;
        }
      }, COL_TOO_HIGH, COL_VALUE);
    }

    if (!Double.isNaN(this.min)) {
      df.mapInPlace(new Series.DoubleConditional() {
        @Override
        public boolean apply(double... values) {
          return values[0] < ThresholdAlgorithm.this.min;
        }
      }, COL_TOO_LOW, COL_VALUE);
    }

    df.mapInPlace(BooleanSeries.HAS_TRUE, COL_ANOMALY, COL_TOO_HIGH, COL_TOO_LOW);

    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    for (int i = 0; i < df.size(); i++) {
      if (BooleanSeries.booleanValueOf(df.getBoolean(COL_ANOMALY, i))) {
        long start = df.getLong(COL_TIME, i);
        long end = getEndTime(df, i);

        anomalies.add(this.makeAnomaly(this.slice.withStart(start).withEnd(end)));
      }
    }

    long maxTime = df.getLongs(COL_TIME).max().longValue();

    return new DetectionPipelineResult(anomalies, maxTime);
  }

  private long getEndTime(DataFrame df, int index) {
    if (index < df.size() - 1) {
      return df.getLong(COL_TIME, index + 1);
    }
    return df.getLongs(COL_TIME).max().longValue() + 1; // TODO use time granularity
  }
}
