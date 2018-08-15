package com.linkedin.thirdeye.detection.algorithm;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.ConfigUtils;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipeline;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import com.linkedin.thirdeye.detector.email.filter.BaseAlertFilter;
import com.linkedin.thirdeye.detector.email.filter.DummyAlertFilter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.collections.MapUtils;


/**
 * The Legacy alert filter wrapper. This wrapper runs the legacy alert filter in detection pipeline and filter out
 * the anomalies that don't pass the filter.
 */
public class LegacyAlertFilterWrapper extends DetectionPipeline {
  private static final String PROP_LEGACY_ALERT_FILTER_CLASS_NAME = "legacyAlertFilterClassName";
  private static final String PROP_NESTED = "nested";
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_ALERT_FILTER_LOOKBACK = "alertFilterLookBack";
  private static String PROP_SPEC = "specs";
  private static String PROP_ALERT_FILTER = "alertFilter";


  private BaseAlertFilter alertFilter;
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

    this.alertFilter = new DummyAlertFilter();
    this.anomalyFunctionSpecs = MapUtils.getMap(config.getProperties(), PROP_SPEC);
    if (config.getProperties().containsKey(PROP_LEGACY_ALERT_FILTER_CLASS_NAME)) {
      String className = MapUtils.getString(config.getProperties(), PROP_LEGACY_ALERT_FILTER_CLASS_NAME);
      this.alertFilter = (BaseAlertFilter) Class.forName(className).newInstance();
      this.alertFilter.setParameters(MapUtils.getMap(this.anomalyFunctionSpecs, PROP_ALERT_FILTER));
    }
    this.nestedProperties = ConfigUtils.getList(config.getProperties().get(PROP_NESTED));
    this.alertFilterLookBack =
        MapUtils.getLong(config.getProperties(), PROP_ALERT_FILTER_LOOKBACK, TimeUnit.DAYS.toMillis(14));
  }

  @Override
  public DetectionPipelineResult run() throws Exception {
    List<MergedAnomalyResultDTO> candidates = new ArrayList<>();
    for (Map<String, Object> properties : this.nestedProperties) {
      DetectionConfigDTO nestedConfig = new DetectionConfigDTO();

      Preconditions.checkArgument(properties.containsKey(PROP_CLASS_NAME), "Nested missing " + PROP_CLASS_NAME);

      properties.put(PROP_SPEC, anomalyFunctionSpecs);
      nestedConfig.setId(this.config.getId());
      nestedConfig.setName(this.config.getName());
      nestedConfig.setProperties(properties);

      DetectionPipeline pipeline =
          this.provider.loadPipeline(nestedConfig, this.startTime - this.alertFilterLookBack, this.endTime);

      DetectionPipelineResult intermediate = pipeline.run();
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
