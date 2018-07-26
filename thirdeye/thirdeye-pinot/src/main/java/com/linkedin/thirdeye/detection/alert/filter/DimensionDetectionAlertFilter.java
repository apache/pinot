package com.linkedin.thirdeye.detection.alert.filter;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.ConfigUtils;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterResult;
import com.linkedin.thirdeye.detection.alert.StatefulDetectionAlertFilter;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.MapUtils;


/**
 * The detection alert filter that sends the anomaly email to a set
 * of unconditional and another set of conditional recipients, based on the value
 * of a specified anomaly dimension
 */
public class DimensionDetectionAlertFilter extends StatefulDetectionAlertFilter {
  private static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";
  private static final String PROP_RECIPIENTS = "recipients";
  private static final String PROP_DIMENSION = "dimension";
  private static final String PROP_DIMENSION_RECIPIENTS = "dimensionRecipients";
  private static final String PROP_SEND_ONCE = "sendOnce";

  final String dimension;
  final List<String> recipients;
  final SetMultimap<String, String> dimensionRecipients;
  final List<Long> detectionConfigIds;
  final boolean sendOnce;

  public DimensionDetectionAlertFilter(DataProvider provider, DetectionAlertConfigDTO config, long endTime) {
    super(provider, config, endTime);
    Preconditions.checkNotNull(config.getProperties().get(PROP_DIMENSION), "Dimension name not specified");

    this.dimension = MapUtils.getString(this.config.getProperties(), PROP_DIMENSION);
    this.recipients = ConfigUtils.getList(this.config.getProperties().get(PROP_RECIPIENTS));
    this.dimensionRecipients = HashMultimap.create(ConfigUtils.<String, String>getMultimap(this.config.getProperties().get(PROP_DIMENSION_RECIPIENTS)));
    this.detectionConfigIds = ConfigUtils.getLongs(this.config.getProperties().get(PROP_DETECTION_CONFIG_IDS));
    this.sendOnce = MapUtils.getBoolean(this.config.getProperties(), PROP_SEND_ONCE, true);
  }

  @Override
  public DetectionAlertFilterResult run(Map<Long, Long> vectorClocks, long highWaterMark) {
    DetectionAlertFilterResult result = new DetectionAlertFilterResult();

    final long minId = getMinId(highWaterMark);

    Set<MergedAnomalyResultDTO> anomalies = this.filter(this.makeVectorClocks(this.detectionConfigIds), minId);

    // group anomalies by dimensions value
    Multimap<String, MergedAnomalyResultDTO> grouped = Multimaps.index(anomalies, new Function<MergedAnomalyResultDTO, String>() {
      @Override
      public String apply(MergedAnomalyResultDTO mergedAnomalyResultDTO) {
        return MapUtils.getString(mergedAnomalyResultDTO.getDimensions(), DimensionDetectionAlertFilter.this.dimension, "");
      }
    });

    // generate recipients-anomalies mapping
    for (Map.Entry<String, Collection<MergedAnomalyResultDTO>> entry : grouped.asMap().entrySet()) {
      Set<String> recipients = this.makeGroupRecipients(entry.getKey());

      result.addMapping(recipients, new HashSet<>(entry.getValue()));
    }

    return result;
  }

  protected Set<String> makeGroupRecipients(String key) {
    Set<String> recipients = new HashSet<>(this.recipients);
    if (this.dimensionRecipients.containsKey(key)) {
      recipients.addAll(this.dimensionRecipients.get(key));
    }
    return recipients;
  }

  private long getMinId(long highWaterMark) {
    if (this.sendOnce) {
      return highWaterMark + 1;
    } else {
      return 0;
    }
  }
}
