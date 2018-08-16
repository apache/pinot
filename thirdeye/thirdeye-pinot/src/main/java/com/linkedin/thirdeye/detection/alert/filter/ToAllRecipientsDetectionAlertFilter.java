package com.linkedin.thirdeye.detection.alert.filter;

import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.ConfigUtils;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterRecipients;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterResult;
import com.linkedin.thirdeye.detection.alert.StatefulDetectionAlertFilter;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.MapUtils;


/**
 * The detection alert filter that sends the anomaly email to all recipients
 */
public class ToAllRecipientsDetectionAlertFilter extends StatefulDetectionAlertFilter {
  private static final String PROP_TO = "to";
  private static final String PROP_CC = "cc";
  private static final String PROP_BCC = "bcc";
  private static final String PROP_DETECTION_CONFIG_IDS = "detectionConfigIds";
  private static final String PROP_SEND_ONCE = "sendOnce";

  Set<String> to;
  Set<String> cc;
  Set<String> bcc;
  List<Long> detectionConfigIds;
  boolean sendOnce;

  public ToAllRecipientsDetectionAlertFilter(DataProvider provider, DetectionAlertConfigDTO config, long endTime) {
    super(provider, config, endTime);

    this.to = new HashSet<>(ConfigUtils.<String>getList(this.config.getProperties().get(PROP_TO)));
    this.cc = new HashSet<>(ConfigUtils.<String>getList(this.config.getProperties().get(PROP_CC)));
    this.bcc = new HashSet<>(ConfigUtils.<String>getList(this.config.getProperties().get(PROP_BCC)));
    this.detectionConfigIds = ConfigUtils.getLongs(this.config.getProperties().get(PROP_DETECTION_CONFIG_IDS));
    this.sendOnce = MapUtils.getBoolean(this.config.getProperties(), PROP_SEND_ONCE, true);
  }

  @Override
  public DetectionAlertFilterResult run(Map<Long, Long> vectorClocks, long highWaterMark) {
    DetectionAlertFilterResult result = new DetectionAlertFilterResult();

    final long minId = getMinId(highWaterMark);

    Set<MergedAnomalyResultDTO> anomalies = this.filter(this.makeVectorClocks(this.detectionConfigIds), minId);

    return result.addMapping(new DetectionAlertFilterRecipients(this.to, this.cc, this.bcc), anomalies);
  }

  private long getMinId(long highWaterMark) {
    if (this.sendOnce) {
      return highWaterMark + 1;
    } else {
      return 0;
    }
  }

}
