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

package org.apache.pinot.thirdeye.detection.algorithm;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipeline;
import org.apache.pinot.thirdeye.detection.DetectionPipelineResult;
import org.apache.pinot.thirdeye.detector.email.filter.BaseAlertFilter;
import org.apache.pinot.thirdeye.detector.email.filter.DummyAlertFilter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.collections4.MapUtils;


/**
 * The Legacy alert filter wrapper. This wrapper runs the legacy alert filter in detection pipeline and filter out
 * the anomalies that don't pass the filter.
 */
public class LegacyAlertFilterWrapper extends DetectionPipeline {
  private static final String PROP_ANOMALY_FUNCTION_CLASS = "anomalyFunctionClassName";
  private static final String PROP_LEGACY_ALERT_FILTER_CLASS_NAME = "legacyAlertFilterClassName";
  private static final String PROP_NESTED = "nested";
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_ALERT_FILTER_LOOKBACK = "alertFilterLookBack";
  private static final String PROP_SPEC = "specs";
  private static final String PROP_ALERT_FILTER = "alertFilter";

  private final BaseAlertFilter alertFilter;
  private final List<Map<String, Object>> nestedProperties;

  // look back of the alert filter runs to pickup new merged anomalies from the past
  private final long alertFilterLookBack;
  private final Map<String, Object> anomalyFunctionSpecs;

  /**
   * Instantiates a new Legacy alert filter wrapper.
   *
   * @param provider the provider
   * @param config the config
   * @param startTime the start time
   * @param endTime the end time
   * @throws Exception the exception
   */
  public LegacyAlertFilterWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime)
      throws Exception {
    super(provider, config, startTime, endTime);

    this.anomalyFunctionSpecs = ConfigUtils.getMap(config.getProperties().get(PROP_SPEC));

    if (config.getProperties().containsKey(PROP_LEGACY_ALERT_FILTER_CLASS_NAME)) {
      String className = MapUtils.getString(config.getProperties(), PROP_LEGACY_ALERT_FILTER_CLASS_NAME);
      this.alertFilter = (BaseAlertFilter) Class.forName(className).newInstance();
      this.alertFilter.setParameters(ConfigUtils.getMap(this.anomalyFunctionSpecs.get(PROP_ALERT_FILTER)));
    } else {
      this.alertFilter = new DummyAlertFilter();
    }

    if (config.getProperties().containsKey(PROP_NESTED)) {
      this.nestedProperties = ConfigUtils.getList(config.getProperties().get(PROP_NESTED));
    } else {
      this.nestedProperties = Collections.singletonList(Collections.singletonMap(PROP_CLASS_NAME, (Object) LegacyMergeWrapper.class.getName()));
    }

    this.alertFilterLookBack = ConfigUtils.parsePeriod(MapUtils.getString(config.getProperties(), PROP_ALERT_FILTER_LOOKBACK, "2week")).toStandardDuration().getMillis();
  }

  @Override
  public DetectionPipelineResult run() throws Exception {
    List<MergedAnomalyResultDTO> candidates = new ArrayList<>();
    for (Map<String, Object> properties : this.nestedProperties) {
      if (!properties.containsKey(PROP_SPEC)) {
        properties.put(PROP_SPEC, this.anomalyFunctionSpecs);
      }
      if (!properties.containsKey(PROP_ANOMALY_FUNCTION_CLASS)) {
        properties.put(PROP_ANOMALY_FUNCTION_CLASS, this.config.getProperties().get(PROP_ANOMALY_FUNCTION_CLASS));
      }
      DetectionPipelineResult intermediate = this.runNested(properties, this.startTime - this.alertFilterLookBack, this.endTime);
      candidates.addAll(intermediate.getAnomalies());
    }

    Collection<MergedAnomalyResultDTO> anomalies =
        Collections2.filter(candidates, new Predicate<MergedAnomalyResultDTO>() {
          @Override
          public boolean apply(@Nullable MergedAnomalyResultDTO mergedAnomaly) {
            return mergedAnomaly != null && !mergedAnomaly.isChild() && alertFilter.isQualified(mergedAnomaly);
          }
        });

    return new DetectionPipelineResult(new ArrayList<>(anomalies));
  }
}
