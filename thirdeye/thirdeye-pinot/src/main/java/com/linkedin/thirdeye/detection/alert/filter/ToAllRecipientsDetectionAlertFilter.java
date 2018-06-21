package com.linkedin.thirdeye.detection.alert.filter;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.AnomalySlice;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.alert.AlertUtils;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterResult;
import com.linkedin.thirdeye.detection.alert.StatefulDetectionAlertFilter;
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

import static com.linkedin.thirdeye.detection.alert.filter.DetectionAlertFilterUtils.*;


/**
 * The detection alert filter that sends the anomaly email to all recipients
 */
public class ToAllRecipientsDetectionAlertFilter extends StatefulDetectionAlertFilter {

  private static final Logger LOG = LoggerFactory.getLogger(ToAllRecipientsDetectionAlertFilter.class);
  private static final String PROP_RECIPIENTS = "recipients";
  private static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";
  private static final String PROP_SEND_ONCE = "sendOnce";

  Set<String> recipients;
  List<Long> detectionConfigIds;
  boolean sendOnce;

  public ToAllRecipientsDetectionAlertFilter(DataProvider provider, DetectionAlertConfigDTO config, long endTime) {
    super(provider, config, endTime);
    Preconditions.checkNotNull(config.getProperties().get(PROP_RECIPIENTS), "Recipients not found.");
    Preconditions.checkArgument(config.getProperties().get(PROP_RECIPIENTS) instanceof Collection, "Read recipients failed.");
    Preconditions.checkNotNull(config.getProperties().get(PROP_DETECTION_CONFIG_IDS), "Detection config ids not found.");
    Preconditions.checkArgument(config.getProperties().get(PROP_DETECTION_CONFIG_IDS) instanceof Collection, "Read detection config ids failed.");

    this.recipients = new HashSet<>((Collection<String>) this.config.getProperties().get(PROP_RECIPIENTS));
    this.detectionConfigIds = extractLongs((Collection<Number>) this.config.getProperties().get(PROP_DETECTION_CONFIG_IDS));
    this.sendOnce = MapUtils.getBoolean(this.config.getProperties(), PROP_SEND_ONCE, true);
  }

  @Override
  public DetectionAlertFilterResult run(Map<Long, Long> vectorClocks, long highWaterMark) {
    DetectionAlertFilterResult result = new DetectionAlertFilterResult();

    final long minId = getMinId(highWaterMark);

    for (Long detectionConfigId : this.detectionConfigIds) {
      long startTime = MapUtils.getLong(vectorClocks, detectionConfigId, 0L);

      AnomalySlice slice = new AnomalySlice().withConfigId(detectionConfigId).withStart(startTime).withEnd(this.endTime);
      Collection<MergedAnomalyResultDTO> candidates = this.provider.fetchAnomalies(Collections.singletonList(slice)).get(slice);

      Collection<MergedAnomalyResultDTO> anomalies =
          Collections2.filter(candidates, new Predicate<MergedAnomalyResultDTO>() {
            @Override
            public boolean apply(@Nullable MergedAnomalyResultDTO mergedAnomalyResultDTO) {
              return mergedAnomalyResultDTO != null
                  && !mergedAnomalyResultDTO.isChild()
                  && !AlertUtils.hasFeedback(mergedAnomalyResultDTO)
                  && (mergedAnomalyResultDTO.getId() == null || mergedAnomalyResultDTO.getId() >= minId);
            }
          });

      result.addMapping(this.recipients, new HashSet<>(anomalies));
    }

    return result;
  }

  private long getMinId(long highWaterMark) {
    if (this.sendOnce) {
      return highWaterMark + 1;
    } else {
      return 0;
    }
  }

}
