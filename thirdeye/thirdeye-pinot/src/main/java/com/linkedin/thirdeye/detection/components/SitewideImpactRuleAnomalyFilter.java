package com.linkedin.thirdeye.detection.components;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dashboard.resources.v2.BaselineParsingUtils;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.detection.InputDataFetcher;
import com.linkedin.thirdeye.detection.Pattern;
import com.linkedin.thirdeye.detection.annotation.Components;
import com.linkedin.thirdeye.detection.annotation.DetectionTag;
import com.linkedin.thirdeye.detection.spec.SitewideImpactRuleAnomalyFilterSpec;
import com.linkedin.thirdeye.detection.spi.components.AnomalyFilter;
import com.linkedin.thirdeye.detection.spi.model.InputData;
import com.linkedin.thirdeye.detection.spi.model.InputDataSpec;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import com.linkedin.thirdeye.rootcause.timeseries.Baseline;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


/**
 * Site-wide impact anomaly filter
 */
@Components(type = "SITEWIDE_IMPACT_FILTER", tags = {DetectionTag.RULE_FILTER})
public class SitewideImpactRuleAnomalyFilter implements AnomalyFilter<SitewideImpactRuleAnomalyFilterSpec> {
  private double threshold;
  private InputDataFetcher dataFetcher;
  private Baseline baseline;
  private String siteWideMetricUrn;
  private Pattern pattern;

  @Override
  public boolean isQualified(MergedAnomalyResultDTO anomaly) {
    MetricEntity me = MetricEntity.fromURN(anomaly.getMetricUrn());
    List<MetricSlice> slices = new ArrayList<>();
    MetricSlice currentSlice = MetricSlice.from(me.getId(), anomaly.getStartTime(), anomaly.getEndTime(), me.getFilters());
    slices.add(currentSlice);

    // customize baseline offset
    MetricSlice baselineSlice = null;
    if (baseline != null) {
      baselineSlice = this.baseline.scatter(currentSlice).get(0);
      slices.add(baselineSlice);
    }

    MetricSlice siteWideSlice;
    if (Strings.isNullOrEmpty(this.siteWideMetricUrn)) {
      // if global metric is not set
      MetricEntity siteWideEntity = MetricEntity.fromURN(anomaly.getMetricUrn());
      siteWideSlice = MetricSlice.from(siteWideEntity.getId(), anomaly.getStartTime(), anomaly.getEndTime());
    } else {
      MetricEntity siteWideEntity = MetricEntity.fromURN(this.siteWideMetricUrn);
      siteWideSlice = MetricSlice.from(siteWideEntity.getId(), anomaly.getStartTime(), anomaly.getEndTime(),
          siteWideEntity.getFilters());
    }
    slices.add(siteWideSlice);



    Map<MetricSlice, DataFrame> aggregates = this.dataFetcher.fetchData(
        new InputDataSpec().withAggregateSlices(slices))
        .getAggregates();

    double currentValue = getValueFromAggregates(currentSlice, aggregates);
    double baselineValue = baselineSlice == null ? anomaly.getAvgBaselineVal() : getValueFromAggregates(baselineSlice, aggregates);
    double siteWideBaselineValue = getValueFromAggregates(siteWideSlice, aggregates);

    // if inconsistent with up/down, filter the anomaly
    if (!pattern.equals(Pattern.UP_OR_DOWN) && (currentValue < baselineValue && pattern.equals(Pattern.UP)) || (currentValue > baselineValue && pattern.equals(Pattern.DOWN))) {
      return false;
    }
    // if doesn't pass the threshold, filter the anomaly
    if (siteWideBaselineValue != 0
        && (Math.abs(currentValue - baselineValue) / siteWideBaselineValue) < this.threshold) {
      return false;
    }

    return true;
  }

  @Override
  public void init(SitewideImpactRuleAnomalyFilterSpec spec, InputDataFetcher dataFetcher) {
    this.dataFetcher = dataFetcher;
    this.threshold = spec.getThreshold();
    Preconditions.checkArgument(Math.abs(this.threshold) <= 1, "Site wide impact threshold should be less or equal than 1");

    this.pattern = Pattern.valueOf(spec.getPattern().toUpperCase());

    // customize baseline offset
    if (StringUtils.isNotBlank(spec.getOffset())){
      this.baseline = BaselineParsingUtils.parseOffset(spec.getOffset(), spec.getTimezone());
    }

    if (!Strings.isNullOrEmpty(spec.getSitewideCollection()) && !Strings.isNullOrEmpty(spec.getSitewideMetricName())) {
      // build filters
      Map<String, Collection<String>> filterMaps = spec.getFilters();
      Multimap<String, String> filters = ArrayListMultimap.create();
      if (filterMaps != null) {
        for (Map.Entry<String, Collection<String>> entry : filterMaps.entrySet()) {
          filters.putAll(entry.getKey(), entry.getValue());
        }
      }

      // build site wide metric Urn
      InputDataSpec.MetricAndDatasetName metricAndDatasetName =
          new InputDataSpec.MetricAndDatasetName(spec.getSitewideMetricName(), spec.getSitewideCollection());
      InputData data = this.dataFetcher.fetchData(
          new InputDataSpec().withMetricNamesAndDatasetNames(Collections.singletonList(metricAndDatasetName)));
      MetricConfigDTO metricConfigDTO = data.getMetricForMetricAndDatasetNames().get(metricAndDatasetName);
      MetricEntity me = MetricEntity.fromMetric(1.0, metricConfigDTO.getId(), filters);
      this.siteWideMetricUrn = me.getUrn();
    }
  }

  private double getValueFromAggregates(MetricSlice slice, Map<MetricSlice, DataFrame> aggregates) {
    return aggregates.get(slice).getDouble(COL_VALUE, 0);
  }
}
