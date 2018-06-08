package com.linkedin.thirdeye.detection.alert.filter;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.AnomalySlice;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilter;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The detection alert filter that sends the anomaly email to a set
 * of unconditional and another set of conditional recipients, based on the value
 * of a specified anomaly dimension
 */
public class DimensionDetectionAlertFilter extends DetectionAlertFilter {

  private static final Logger LOG = LoggerFactory.getLogger(DimensionDetectionAlertFilter.class);
  private static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";
  private static final String PROP_RECIPIENTS = "recipients";
  private static final String PROP_DIMENSION = "dimension";
  private static final String PROP_DIMENSION_RECIPIENTS = "dimensionRecipients";

  final String dimension;
  final List<String> recipients;
  final SetMultimap<String, String> dimensionRecipients;
  final List<Long> detectionConfigIds;
  final Map<Long, Long> vectorClocks;

  public DimensionDetectionAlertFilter(DataProvider provider, DetectionAlertConfigDTO config, long endTime) {
    super(provider, config, endTime);
    Preconditions.checkNotNull(config.getProperties().get(PROP_DIMENSION), "Dimension name not specified");
    Preconditions.checkNotNull(config.getProperties().get(PROP_RECIPIENTS), "Recipients not found.");
    Preconditions.checkArgument(config.getProperties().get(PROP_RECIPIENTS) instanceof Collection, "Read recipients failed.");
    Preconditions.checkNotNull(config.getProperties().get(PROP_DIMENSION_RECIPIENTS), "Dimension recipients not found.");
    Preconditions.checkArgument(config.getProperties().get(PROP_DIMENSION_RECIPIENTS) instanceof Map, "Read dimension recipients failed.");
    Preconditions.checkNotNull(config.getProperties().get(PROP_DETECTION_CONFIG_IDS), "Detection config ids not found.");
    Preconditions.checkArgument(config.getProperties().get(PROP_DETECTION_CONFIG_IDS) instanceof Collection, "Read detection config ids failed.");

    this.dimension = MapUtils.getString(this.config.getProperties(), PROP_DIMENSION);
    this.recipients = new ArrayList<>((Collection<String>) this.config.getProperties().get(PROP_RECIPIENTS));
    this.dimensionRecipients = extractNestedMap((Map<String, Collection<String>>) this.config.getProperties().get(PROP_DIMENSION_RECIPIENTS));
    this.detectionConfigIds = extractLongs((Collection<Number>) this.config.getProperties().get(PROP_DETECTION_CONFIG_IDS));
    this.vectorClocks = this.config.getVectorClocks();
  }

  @Override
  public DetectionAlertFilterResult run() {
    DetectionAlertFilterResult result = new DetectionAlertFilterResult();

    // retrieve all candidate anomalies
    Set<MergedAnomalyResultDTO> allAnomalies = new HashSet<>();
    for (Long detectionConfigId : this.detectionConfigIds) {
      long startTime = MapUtils.getLong(this.vectorClocks, detectionConfigId, 0L);

      AnomalySlice slice = new AnomalySlice().withConfigId(detectionConfigId).withStart(startTime).withEnd(this.endTime);
      Collection<MergedAnomalyResultDTO> candidates = this.provider.fetchAnomalies(Collections.singletonList(slice)).get(slice);

      Collection<MergedAnomalyResultDTO> anomalies =
          Collections2.filter(candidates, new Predicate<MergedAnomalyResultDTO>() {
            @Override
            public boolean apply(@Nullable MergedAnomalyResultDTO mergedAnomalyResultDTO) {
              return mergedAnomalyResultDTO != null && !mergedAnomalyResultDTO.isChild();
            }
          });

      allAnomalies.addAll(anomalies);

      this.vectorClocks.put(detectionConfigId, getLastTimeStamp(anomalies, startTime));
    }

    // update last timestamp(s)
    result.setVectorClocks(this.vectorClocks);

    // group anomalies by dimensions value
    Multimap<String, MergedAnomalyResultDTO> grouped = Multimaps.index(allAnomalies, new Function<MergedAnomalyResultDTO, String>() {
      @Override
      public String apply(MergedAnomalyResultDTO mergedAnomalyResultDTO) {
        return MapUtils.getString(mergedAnomalyResultDTO.getDimensions(), DimensionDetectionAlertFilter.this.dimension, "");
      }
    });

    // generate recipients-anomalies mapping
    for (Map.Entry<String, Collection<MergedAnomalyResultDTO>> entry : grouped.asMap().entrySet()) {
      Set<String> receipients = new HashSet<>(this.recipients);
      if (this.dimensionRecipients.containsKey(entry.getKey())) {
        receipients.addAll(this.dimensionRecipients.get(entry.getKey()));
      }

      result.addMapping(receipients, new HashSet<>(entry.getValue()));
    }

    return result;
  }

  private Long getLastTimeStamp(Collection<MergedAnomalyResultDTO> anomalies, long startTime) {
    long lastTimeStamp = startTime;
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      lastTimeStamp = Math.max(anomaly.getEndTime(), lastTimeStamp);
    }
    return lastTimeStamp;
  }

  private static List<Long> extractLongs(Collection<Number> numbers) {
    List<Long> output = new ArrayList<>();
    for (Number n : numbers) {
      if (n == null) {
        continue;
      }
      output.add(n.longValue());
    }
    return output;
  }

  private static SetMultimap<String, String> extractNestedMap(Map<String, Collection<String>> nestedMap) {
    SetMultimap<String, String> output = HashMultimap.create();
    for (Map.Entry<String, Collection<String>> entry : nestedMap.entrySet()) {
      for (String value : entry.getValue()) {
        output.put(entry.getKey(), value);
      }
    }
    return output;
  }
}
