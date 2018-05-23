package com.linkedin.thirdeye.detection.algorithm;

import com.google.common.base.Preconditions;
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
import com.linkedin.thirdeye.rootcause.timeseries.Baseline;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineAggregate;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineAggregateType;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections.MapUtils;
import org.joda.time.DateTimeZone;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


/**
 * Simple baseline algorithm. Computes a multi-week aggregate baseline and compares
 * the current value based on relative change or absolute difference.
 */
public class BaselineAlgorithm extends StaticDetectionPipeline {
  private static final String COL_CURR = "current";
  private static final String COL_BASE = "baseline";
  private static final String COL_DIFF = "diff";
  private static final String COL_DIFF_VIOLATION = "diff_violation";
  private static final String COL_CHANGE = "change";
  private static final String COL_CHANGE_VIOLATION = "change_violation";
  private static final String COL_ANOMALY = "anomaly";

  private static final String PROP_METRIC_URN = "metricUrn";

  private static final String PROP_AGGREGATION = "aggregation";
  private static final String PROP_AGGREGATION_DEFAULT = "MEDIAN";

  private static final String PROP_WEEKS = "weeks";
  private static final int PROP_WEEKS_DEFAULT = 1;

  private static final String PROP_CHANGE = "change";
  private static final double PROP_CHANGE_DEFAULT = Double.NaN;

  private static final String PROP_DIFFERENCE = "difference";
  private static final double PROP_DIFFERENCE_DEFAULT = Double.NaN;

  private static final String PROP_TIMEZONE = "timezone";
  private static final String PROP_TIMEZONE_DEFAULT = "UTC";

  private MetricSlice slice;
  private Baseline baseline;
  private double change;
  private double difference;

  public BaselineAlgorithm(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    super(provider, config, startTime, endTime);

    Preconditions.checkArgument(config.getProperties().containsKey(PROP_METRIC_URN));

    String metricUrn = MapUtils.getString(config.getProperties(), PROP_METRIC_URN);
    MetricEntity me = MetricEntity.fromURN(metricUrn, 1.0);
    this.slice = MetricSlice.from(me.getId(), this.startTime, this.endTime, me.getFilters());

    int weeks = MapUtils.getIntValue(config.getProperties(), PROP_WEEKS, PROP_WEEKS_DEFAULT);
    BaselineAggregateType aggregation = BaselineAggregateType.valueOf(MapUtils.getString(config.getProperties(), PROP_AGGREGATION, PROP_AGGREGATION_DEFAULT));
    DateTimeZone timezone = DateTimeZone.forID(MapUtils.getString(this.config.getProperties(), PROP_TIMEZONE, PROP_TIMEZONE_DEFAULT));
    this.baseline = BaselineAggregate.fromWeekOverWeek(aggregation, weeks, 1, timezone);

    this.change = MapUtils.getDoubleValue(config.getProperties(), PROP_CHANGE, PROP_CHANGE_DEFAULT);
    this.difference = MapUtils.getDoubleValue(config.getProperties(), PROP_DIFFERENCE, PROP_DIFFERENCE_DEFAULT);
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
    DataFrame dfCurr = data.getTimeseries().get(this.slice).renameSeries(COL_VALUE, COL_CURR);
    DataFrame dfBase = this.baseline.gather(this.slice, data.getTimeseries()).renameSeries(COL_VALUE, COL_BASE);

    DataFrame df = new DataFrame(dfCurr).addSeries(dfBase);
    df.addSeries(COL_DIFF, df.getDoubles(COL_CURR).subtract(df.get(COL_BASE)));
    df.addSeries(COL_CHANGE, df.getDoubles(COL_CURR).divide(df.get(COL_BASE)).subtract(1));

    // relative change
    df.addSeries(COL_CHANGE_VIOLATION, BooleanSeries.fillValues(df.size(), false));
    if (!Double.isNaN(this.change)) {
      df.mapInPlace(new Series.DoubleConditional() {
        @Override
        public boolean apply(double... values) {
          return Math.abs(values[0]) >= BaselineAlgorithm.this.change;
        }
      }, COL_CHANGE_VIOLATION, COL_CHANGE);
    }

    // absolute difference
    df.addSeries(COL_DIFF_VIOLATION, BooleanSeries.fillValues(df.size(), false));
    if (!Double.isNaN(this.difference)) {
      df.mapInPlace(new Series.DoubleConditional() {
        @Override
        public boolean apply(double... values) {
          return Math.abs(values[0]) >= BaselineAlgorithm.this.difference;
        }
      }, COL_DIFF_VIOLATION, COL_DIFF);
    }

    // anomalies
    df.mapInPlace(BooleanSeries.HAS_TRUE, COL_ANOMALY, COL_CHANGE_VIOLATION, COL_DIFF_VIOLATION);

    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    for (int i = 0; i < df.size(); i++) {
      if (BooleanSeries.booleanValueOf(df.getBoolean(COL_ANOMALY, i))) {
        long start = df.getLong(COL_TIME, i);
        long end = getEndTime(df, i);

        anomalies.add(this.makeAnomaly(this.slice.withStart(start).withEnd(end)));
      }
    }

    long maxTime = -1;
    if (!df.isEmpty()) {
      maxTime = df.getLongs(COL_TIME).max().longValue();
    }

    return new DetectionPipelineResult(anomalies, maxTime);
  }

  private long getEndTime(DataFrame df, int index) {
    if (index < df.size() - 1) {
      return df.getLong(COL_TIME, index + 1);
    }
    return df.getLongs(COL_TIME).max().longValue() + 1; // TODO use time granularity
  }
}
