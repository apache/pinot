/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.detection;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.loader.AggregationLoader;
import org.apache.pinot.thirdeye.rootcause.timeseries.Baseline;
import org.apache.pinot.thirdeye.rootcause.timeseries.BaselineAggregate;
import org.apache.pinot.thirdeye.rootcause.timeseries.BaselineAggregateType;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTimeZone;

/**
 * The Current and baseline loader.
 */
public class CurrentAndBaselineLoader {
  private static final long TIMEOUT = 60000;

  private MetricConfigManager metricDAO;
  private DatasetConfigManager datasetDAO;
  private AggregationLoader aggregationLoader;

  /**
   * Instantiates a new Current and baseline loader.
   *
   * @param metricDAO the metric dao
   * @param datasetDAO the dataset dao
   * @param aggregationLoader the aggregation loader
   */
  public CurrentAndBaselineLoader(MetricConfigManager metricDAO, DatasetConfigManager datasetDAO,
      AggregationLoader aggregationLoader) {
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.aggregationLoader = aggregationLoader;
  }

  /**
   * Fill in current and baseline value in the anomalies.
   *
   * @param anomalies the anomalies
   * @throws Exception the exception
   */
  public void fillInCurrentAndBaselineValue(Collection<MergedAnomalyResultDTO> anomalies) throws Exception {
    ExecutorService executor = Executors.newCachedThreadPool();

    for (final MergedAnomalyResultDTO anomaly : anomalies) {
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

        final MetricSlice slice = MetricSlice.from(metricConfigDTO.getId(), anomaly.getStartTime(), anomaly.getEndTime(), filters);

        DateTimeZone timezone = getDateTimeZone(datasetConfigDTO);
        final Baseline baseline = BaselineAggregate.fromWeekOverWeek(BaselineAggregateType.SUM, 1, 1, timezone);

        executor.submit(new Runnable() {
          @Override
          public void run() {
            anomaly.setAvgCurrentVal(getAggregate(slice));
            anomaly.setAvgBaselineVal(getAggregate(baseline.scatter(slice).get(0)));
          }
        });
      }
    }

    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
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

  private double getAggregate(MetricSlice slice) {
    try {
      return this.aggregationLoader.loadAggregate(slice, Collections.<String>emptyList(), -1).getDouble(
          DataFrame.COL_VALUE, 0);
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
