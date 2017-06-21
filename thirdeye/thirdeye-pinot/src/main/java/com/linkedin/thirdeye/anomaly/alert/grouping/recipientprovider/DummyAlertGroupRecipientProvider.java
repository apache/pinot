package com.linkedin.thirdeye.anomaly.alert.grouping.recipientprovider;

import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.List;

public class DummyAlertGroupRecipientProvider extends BaseAlertGroupRecipientProvider {
  @Override
  public String getAlertGroupRecipients(DimensionMap dimensions, List<MergedAnomalyResultDTO> anomalyResultList) {
    return EMPTY_RECIPIENTS;
  }
}
