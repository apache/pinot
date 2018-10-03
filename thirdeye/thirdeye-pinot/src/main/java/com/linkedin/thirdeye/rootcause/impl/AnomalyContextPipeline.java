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

package com.linkedin.thirdeye.rootcause.impl;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The AnomalyContextPipeline resolves an anomaly entity to a rootcause search context that can serve as input to a typical RCA framework.
 * Populates time ranges, metric urn, and dimension filters.
 */
public class AnomalyContextPipeline extends Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyContextPipeline.class);

  private static final String PROP_BASELINE_OFFSET = "baselineOffset";
  private static final long PROP_BASELINE_OFFSET_DEFAULT = TimeUnit.DAYS.toMillis(7);

  private static final String PROP_ANALYSIS_WINDOW = "analysisWindow";
  private static final long PROP_ANALYSIS_WINDOW_DEFAULT = TimeUnit.DAYS.toMillis(14);

  private final MergedAnomalyResultManager anomalyDAO;
  private final MetricConfigManager metricDAO;

  private final long baselineOffset;
  private final long analysisWindow;

  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param anomalyDAO anomaly config DAO
   * @param metricDAO metric config DAO
   * @param baselineOffset baseline range offset
   * @param analysisWindow analysis range window up to end of anomaly
   */
  public AnomalyContextPipeline(String outputName, Set<String> inputNames, MergedAnomalyResultManager anomalyDAO, MetricConfigManager metricDAO, long baselineOffset, long analysisWindow) {
    super(outputName, inputNames);
    this.anomalyDAO = anomalyDAO;
    this.metricDAO = metricDAO;
    this.baselineOffset = baselineOffset;
    this.analysisWindow = analysisWindow;
  }

  /**
   * Alternate constructor for use by RCAFrameworkLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param properties configuration properties ({@code PROP_BASELINE_OFFSET}, {@code PROP_ANALYSIS_WINDOW})
   */
  public AnomalyContextPipeline(String outputName, Set<String> inputNames, Map<String, Object> properties) {
    super(outputName, inputNames);
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.baselineOffset = MapUtils.getLongValue(properties, PROP_BASELINE_OFFSET, PROP_BASELINE_OFFSET_DEFAULT);
    this.analysisWindow = MapUtils.getLongValue(properties, PROP_ANALYSIS_WINDOW, PROP_ANALYSIS_WINDOW_DEFAULT);
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    Set<AnomalyEventEntity> anomalies = context.filter(AnomalyEventEntity.class);

    if (anomalies.size() > 1) {
      LOG.warn("Got multiple anomalies to resolve. This could lead to unexpected results.");
    }

    Set<Entity> output = new HashSet<>();
    for (AnomalyEventEntity e : anomalies) {
      MergedAnomalyResultDTO anomalyDTO = this.anomalyDAO.findById(e.getId());
      if (anomalyDTO == null) {
        LOG.warn("Could not resolve anomaly id {}. Skipping.", e.getId());
        continue;
      }

      long start = anomalyDTO.getStartTime();
      long end = anomalyDTO.getEndTime();

      // TODO replace with metric id when available
      String metric = anomalyDTO.getMetric();
      String dataset = anomalyDTO.getCollection();

      MetricConfigDTO metricDTO = this.metricDAO.findByMetricAndDataset(metric, dataset);
      if (metricDTO == null) {
        LOG.warn("Could not resolve metric '{}' from '{}'", metric, dataset);
        continue;
      }

      long metricId = metricDTO.getId();

      // time ranges
      output.add(TimeRangeEntity.fromRange(1.0, TimeRangeEntity.TYPE_ANOMALY, start, end));
      output.add(TimeRangeEntity.fromRange(0.8, TimeRangeEntity.TYPE_BASELINE, start - this.baselineOffset, end - this.baselineOffset));
      output.add(TimeRangeEntity.fromRange(1.0, TimeRangeEntity.TYPE_ANALYSIS, end - this.analysisWindow, end));

      // filters
      Multimap<String, String> filters = TreeMultimap.create();
      for (Map.Entry<String, String> entry : anomalyDTO.getDimensions().entrySet()) {
        filters.put(entry.getKey(), entry.getValue());

        // TODO deprecate dimension entity?
        output.add(DimensionEntity.fromDimension(1.0, entry.getKey(), entry.getValue(), DimensionEntity.TYPE_PROVIDED));
      }

      // metric
      output.add(MetricEntity.fromMetric(1.0, metricId, filters));

    }

    return new PipelineResult(context, output);
  }
}
