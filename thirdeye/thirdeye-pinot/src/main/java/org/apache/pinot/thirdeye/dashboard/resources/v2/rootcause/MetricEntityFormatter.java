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

package org.apache.pinot.thirdeye.dashboard.resources.v2.rootcause;

import com.google.common.cache.LoadingCache;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.dashboard.resources.v2.ResourceUtils;
import org.apache.pinot.thirdeye.dashboard.resources.v2.RootCauseEntityFormatter;
import org.apache.pinot.thirdeye.dashboard.resources.v2.pojo.RootCauseEntity;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.rootcause.Entity;
import org.apache.pinot.thirdeye.rootcause.util.EntityUtils;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.apache.pinot.thirdeye.rootcause.impl.TimeRangeEntity;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MetricEntityFormatter extends RootCauseEntityFormatter {
  private static final Logger LOG = LoggerFactory.getLogger(MetricEntityFormatter.class);

  private static final long SLICE_START_OFFSET = TimeUnit.DAYS.toMillis(7);

  private static final Map<String, Integer> TIME_RANGE_PRIORITY = new HashMap<>();
  static {
    TIME_RANGE_PRIORITY.put(TimeRangeEntity.TYPE_ANALYSIS, 0);
    TIME_RANGE_PRIORITY.put(TimeRangeEntity.TYPE_ANOMALY, 1);
    TIME_RANGE_PRIORITY.put(TimeRangeEntity.TYPE_BASELINE, 2);
  }

  public static final String TYPE_METRIC = "metric";

  public static final String ATTR_DATASET = "dataset";
  public static final String ATTR_INVERSE = "inverse";
  public static final String ATTR_DERIVED = "derived";
  public static final String ATTR_ADDITIVE = "additive";
  public static final String ATTR_GRANULARITY = "granularity";
  public static final String ATTR_MAX_TIME = "maxTime";
  public static final String ATTR_EXTERNAL_URLS = "externalUrls";

  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final LoadingCache<String, Long> maxTimeCache;

  public MetricEntityFormatter(MetricConfigManager metricDAO, DatasetConfigManager datasetDAO, LoadingCache<String, Long> maxTimeCache) {
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.maxTimeCache = maxTimeCache;
  }

  public MetricEntityFormatter() {
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.maxTimeCache = ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache();
  }

  @Override
  public boolean applies(Entity entity) {
    return entity instanceof MetricEntity;
  }

  @Override
  public RootCauseEntity format(Entity entity) {
    MetricEntity e = (MetricEntity) entity;

    MetricConfigDTO metric = this.metricDAO.findById(e.getId());
    if (metric == null) {
      throw new IllegalArgumentException(String.format("Could not resolve metric id %d", e.getId()));
    }

    DatasetConfigDTO dataset = this.datasetDAO.findByDataset(metric.getDataset());
    if (dataset == null) {
      throw new IllegalArgumentException(String.format("Could not resolve dataset '%s' for metric id %d", metric.getDataset(), metric.getId()));
    }

    long maxTime = -1;
    try {
      // TODO adjust this +1 once maxTime cache returns end exclusive
      maxTime = this.maxTimeCache.get(dataset.getDataset()) + 1;
    } catch (Exception ex) {
      // left blank
    }

    Multimap<String, String> attributes = ArrayListMultimap.create();
    attributes.put(ATTR_DATASET, dataset.getName());
    attributes.put(ATTR_INVERSE, String.valueOf(metric.isInverseMetric()));
    attributes.put(ATTR_DERIVED, String.valueOf(metric.isDerived()));
    attributes.put(ATTR_ADDITIVE, String.valueOf(dataset.isAdditive()));
    attributes.put(ATTR_GRANULARITY, dataset.bucketTimeGranularity().toAggregationGranularityString());
    attributes.put(ATTR_MAX_TIME, String.valueOf(maxTime));

    TimeRangeEntity range = estimateTimeRange(e);
    MetricSlice slice = MetricSlice.from(metric.getId(), range.getStart(), range.getEnd(), e.getFilters());
    Map<String, String> externalUrls = ResourceUtils.getExternalURLs(slice, this.metricDAO, this.datasetDAO);

    attributes.putAll(ATTR_EXTERNAL_URLS, externalUrls.keySet());
    for (Map.Entry<String, String> entry : externalUrls.entrySet()) {
      attributes.put(entry.getKey(), entry.getValue());
    }

    String label = String.format("%s::%s", dataset.getName(), metric.getName());

    RootCauseEntity out = makeRootCauseEntity(entity, TYPE_METRIC, label, null);
    out.setAttributes(attributes);

    return out;
  }

  /**
   * Estimates the time range for display in the external link from available information.
   * Attempts to extract related time range entities, in order of {@code TIME_RANGE_PRIORITY}, and
   * if not found, falls back to the current system timestamp minus {@code SLICE_START_OFFSET}.
   *
   * @param metric metric entity
   * @return time range entity
   */
  private static TimeRangeEntity estimateTimeRange(MetricEntity metric) {
    TimeRangeEntity range = extractLowest(EntityUtils.filter(metric.getRelated(), TimeRangeEntity.class));

    if (range == null) {
      long end = System.currentTimeMillis();
      long start = end - SLICE_START_OFFSET;
      return TimeRangeEntity.fromRange(1.0, "custom", start, end);
    }

    return range;
  }

  private static TimeRangeEntity extractLowest(Iterable<TimeRangeEntity> ranges) {
    TimeRangeEntity out = null;
    int priority = Integer.MAX_VALUE;
    for (TimeRangeEntity r : ranges) {
      final int rPriority = TIME_RANGE_PRIORITY.get(r.getType());
      if (rPriority < priority) {
        priority = rPriority;
        out = r;
      }
    }
    return out;
  }
}
