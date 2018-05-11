package com.linkedin.thirdeye.detection.alert.filter;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.AnomalySlice;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilter;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;


public class ToAllRecipientsDetectionAlertFilter extends DetectionAlertFilter {

  private static final String PROP_RECIPIENTS = "recipients";
  private static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";

  List<String> recipients;
  List<Long> detectionConfigIds;

  public ToAllRecipientsDetectionAlertFilter(DataProvider provider, AlertConfigDTO config, long startTime,
      long endTime) {
    super(provider, config, startTime, endTime);
    Preconditions.checkNotNull(config.getProperties().get(PROP_RECIPIENTS), "Recipients not found.");
    Preconditions.checkArgument(config.getProperties().get(PROP_RECIPIENTS) instanceof List, "Read recipients failed.");
    Preconditions.checkNotNull(config.getProperties().get(PROP_DETECTION_CONFIG_IDS), "Detection config ids not found.");
    Preconditions.checkArgument(config.getProperties().get(PROP_DETECTION_CONFIG_IDS) instanceof List, "Read detection config ids failed.");

    this.recipients = (List<String>) this.config.getProperties().get(PROP_RECIPIENTS);
    this.detectionConfigIds = (List<Long>) this.config.getProperties().get(PROP_DETECTION_CONFIG_IDS);
  }

  @Override
  public DetectionAlertFilterResult run() {
    List<MergedAnomalyResultDTO> candidates = new ArrayList<>();

    for (Long detectionConfigId : this.detectionConfigIds){
      AnomalySlice slice = new AnomalySlice().withConfigId(detectionConfigId).withStart(this.startTime).withEnd(this.endTime);
      candidates.addAll(this.provider.fetchAnomalies(Collections.singletonList(slice)).get(slice));
    }

    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>(
        Collections2.filter(candidates, new Predicate<MergedAnomalyResultDTO>() {
          @Override
          public boolean apply(@Nullable MergedAnomalyResultDTO mergedAnomalyResultDTO) {
            return mergedAnomalyResultDTO != null
                && !mergedAnomalyResultDTO.isChild();
          }
        }));

    DetectionAlertFilterResult result = new DetectionAlertFilterResult();
    if (CollectionUtils.isNotEmpty(anomalies)) {
      result.addMapping(anomalies, this.recipients);
    }
    return result;
  }
}
