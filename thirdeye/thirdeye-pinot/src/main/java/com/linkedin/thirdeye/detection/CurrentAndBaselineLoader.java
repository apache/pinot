package com.linkedin.thirdeye.detection;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dashboard.resources.v2.aggregation.AggregationLoader;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.rootcause.timeseries.Baseline;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineAggregate;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineAggregateType;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTimeZone;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


public class CurrentAndBaselineLoader {

  private MetricConfigManager metricDAO;
  private DatasetConfigManager datasetDAO;
  private AggregationLoader aggregationLoader;

  public CurrentAndBaselineLoader(MetricConfigManager metricDAO, DatasetConfigManager datasetDAO,
      AggregationLoader aggregationLoader) {
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.aggregationLoader = aggregationLoader;
  }

  public void fillInCurrentAndBaselineValue(Collection<MergedAnomalyResultDTO> anomalies) throws Exception {
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      if (anomaly.getAvgBaselineVal() == 0 || anomaly.getAvgCurrentVal() == 0) {
        MetricConfigDTO metricConfigDTO =
            this.metricDAO.findByMetricAndDataset(anomaly.getMetric(), anomaly.getCollection());
        if (metricConfigDTO == null) {
          throw new IllegalArgumentException(
              String.format("Could not resolve metric '%s' and dataset '%s'", anomaly.getMetric(),
                  anomaly.getCollection()));
        }

        DatasetConfigDTO datasetConfigDTO = this.datasetDAO.findByDataset(anomaly.getCollection());
        if (datasetConfigDTO == null) {
          throw new IllegalArgumentException(String.format("Could not dataset '%s'", anomaly.getCollection()));
        }

        Multimap<String, String> filters = getFiltersFromDimensionMaps(anomaly);

        MetricSlice slice =
            MetricSlice.from(metricConfigDTO.getId(), anomaly.getStartTime(), anomaly.getEndTime(), filters);
        anomaly.setAvgCurrentVal(getAggregate(slice));

        DateTimeZone timezone = getDateTimeZone(datasetConfigDTO);

        Baseline baseline = BaselineAggregate.fromWeekOverWeek(BaselineAggregateType.MEDIAN, 1, 1, timezone);
        anomaly.setAvgBaselineVal(getAggregate(baseline.scatter(slice).get(0)));
      }
    }
  }

  private DateTimeZone getDateTimeZone(DatasetConfigDTO datasetConfigDTO) {
    try {
      if (StringUtils.isBlank(datasetConfigDTO.getTimezone())) {
        return DateTimeZone.forID(datasetConfigDTO.getTimezone());
      }
    } catch (Exception ignore) {
      // ignored
    }
    return DateTimeZone.UTC;
  }

  private double getAggregate(MetricSlice slice) throws Exception {
    DataFrame df = this.aggregationLoader.loadAggregate(slice, Collections.<String>emptyList());
    try {
      return df.getDouble(COL_VALUE, 0);
    } catch (Exception e) {
      return Double.NaN;
    }
  }

  private Multimap<String, String> getFiltersFromDimensionMaps(MergedAnomalyResultDTO anomaly) {
    Multimap<String, String> filters = ArrayListMultimap.create();
    for (Map.Entry<String, String> entry : anomaly.getDimensions().entrySet()) {
      filters.put(entry.getKey(), entry.getValue());
    }
    return filters;
  }
}
