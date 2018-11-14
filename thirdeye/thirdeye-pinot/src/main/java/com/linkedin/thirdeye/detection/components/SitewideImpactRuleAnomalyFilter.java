package com.linkedin.thirdeye.detection.components;

import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.linkedin.thirdeye.dashboard.resources.v2.BaselineParsingUtils;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.InputDataFetcher;
import com.linkedin.thirdeye.detection.annotation.Components;
import com.linkedin.thirdeye.detection.spec.SitewideImpactRuleAnomalyFilterSpec;
import com.linkedin.thirdeye.detection.spi.components.AnomalyFilter;
import com.linkedin.thirdeye.detection.spi.model.InputDataSpec;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import com.linkedin.thirdeye.rootcause.timeseries.Baseline;
import java.util.Arrays;
import java.util.Map;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;

/**
 * Site-wide impact anomaly filter
 */
@Components(type = "SITEWIDE_IMPACT_FILTER")
public class SitewideImpactRuleAnomalyFilter implements AnomalyFilter<SitewideImpactRuleAnomalyFilterSpec> {
  private double threshold;
  private InputDataFetcher dataFetcher;
  private Baseline baseline;
  private String siteWideMetricUrn;

  @Override
  public boolean isQualified(MergedAnomalyResultDTO anomaly) {
    MetricEntity me = MetricEntity.fromURN(anomaly.getMetricUrn());
    MetricSlice currentSlice =
        MetricSlice.from(me.getId(), anomaly.getStartTime(), anomaly.getEndTime(), me.getFilters());
    MetricSlice baselineSlice = this.baseline.scatter(currentSlice).get(0);

    String siteWideImpactMetricUrn = Strings.isNullOrEmpty(this.siteWideMetricUrn) ? anomaly.getMetricUrn() : this.siteWideMetricUrn;
    MetricEntity siteWideEntity = MetricEntity.fromURN(siteWideImpactMetricUrn).withFilters(ArrayListMultimap.<String, String>create());
    MetricSlice siteWideSlice = this.baseline.scatter(MetricSlice.from(siteWideEntity.getId(), anomaly.getStartTime(), anomaly.getEndTime())).get(0);

    Map<MetricSlice, DataFrame> aggregates =
        this.dataFetcher.fetchData(new InputDataSpec().withAggregateSlices(Arrays.asList(currentSlice, baselineSlice, siteWideSlice))).getAggregates();

    double currentValue = getValueFromAggregates(currentSlice, aggregates);
    double baselineValue = getValueFromAggregates(baselineSlice, aggregates);
    double siteWideBaselineValue = getValueFromAggregates(siteWideSlice, aggregates);

    if (siteWideBaselineValue != 0 && (Math.abs(currentValue - baselineValue) / siteWideBaselineValue) < this.threshold) {
      return false;
    }

    return true;
  }

  @Override
  public void init(SitewideImpactRuleAnomalyFilterSpec spec, InputDataFetcher dataFetcher) {
    this.dataFetcher = dataFetcher;
    this.baseline = BaselineParsingUtils.parseOffset(spec.getOffset(), spec.getTimezone());
    this.threshold = spec.getThreshold();
    this.siteWideMetricUrn = spec.getSitewideMetricUrn();
  }

  private double getValueFromAggregates(MetricSlice slice, Map<MetricSlice, DataFrame> aggregates) {
    return aggregates.get(slice).getDouble(COL_VALUE, 0);
  }
}
