/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.detection.algorithm;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.dataframe.BooleanSeries;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import com.linkedin.thirdeye.detection.StaticDetectionPipeline;
import com.linkedin.thirdeye.detection.spi.model.InputData;
import com.linkedin.thirdeye.detection.spi.model.InputDataSpec;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import com.linkedin.thirdeye.rootcause.timeseries.Baseline;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineAggregate;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineAggregateType;
import java.util.ArrayList;
import java.util.Collections;
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
    MetricEntity me = MetricEntity.fromURN(metricUrn);
    this.slice = MetricSlice.from(me.getId(), this.startTime, this.endTime, me.getFilters());

    int weeks = MapUtils.getIntValue(config.getProperties(), PROP_WEEKS, PROP_WEEKS_DEFAULT);
    BaselineAggregateType aggregation = BaselineAggregateType.valueOf(MapUtils.getString(config.getProperties(), PROP_AGGREGATION, PROP_AGGREGATION_DEFAULT));
    DateTimeZone timezone = DateTimeZone.forID(MapUtils.getString(this.config.getProperties(), PROP_TIMEZONE, PROP_TIMEZONE_DEFAULT));
    this.baseline = BaselineAggregate.fromWeekOverWeek(aggregation, weeks, 1, timezone);

    this.change = MapUtils.getDoubleValue(config.getProperties(), PROP_CHANGE, PROP_CHANGE_DEFAULT);
    this.difference = MapUtils.getDoubleValue(config.getProperties(), PROP_DIFFERENCE, PROP_DIFFERENCE_DEFAULT);
  }

  @Override
  public InputDataSpec getInputDataSpec() {
    List<MetricSlice> slices = new ArrayList<>(this.baseline.scatter(this.slice));
    slices.add(this.slice);

    return new InputDataSpec()
        .withTimeseriesSlices(slices);
  }

  @Override
  public DetectionPipelineResult run(InputData data) {
    DataFrame dfCurr = data.getTimeseries().get(this.slice).renameSeries(COL_VALUE, COL_CURR);
    DataFrame dfBase = this.baseline.gather(this.slice, data.getTimeseries()).renameSeries(COL_VALUE, COL_BASE);

    DataFrame df = new DataFrame(dfCurr).addSeries(dfBase);
    df.addSeries(COL_DIFF, df.getDoubles(COL_CURR).subtract(df.get(COL_BASE)));
    df.addSeries(COL_CHANGE, df.getDoubles(COL_CURR).divide(df.get(COL_BASE)).subtract(1));

    // defaults
    df.addSeries(COL_CHANGE_VIOLATION, BooleanSeries.fillValues(df.size(), false));
    df.addSeries(COL_DIFF_VIOLATION, BooleanSeries.fillValues(df.size(), false));

    // relative change
    if (!Double.isNaN(this.change)) {
      df.addSeries(COL_CHANGE_VIOLATION, df.getDoubles(COL_CHANGE).abs().gte(this.change));
    }

    // absolute difference
    if (!Double.isNaN(this.difference)) {
      df.addSeries(COL_DIFF_VIOLATION, df.getDoubles(COL_DIFF).abs().gte(this.difference));
    }

    // anomalies
    df.mapInPlace(BooleanSeries.HAS_TRUE, COL_ANOMALY, COL_CHANGE_VIOLATION, COL_DIFF_VIOLATION);

    List<MergedAnomalyResultDTO> anomalies = this.makeAnomalies(this.slice, df, COL_ANOMALY);

    return new DetectionPipelineResult(anomalies)
        .setDiagnostics(Collections.singletonMap(DetectionPipelineResult.DIAGNOSTICS_DATA, (Object) df.dropAllNullColumns()));
  }
}
