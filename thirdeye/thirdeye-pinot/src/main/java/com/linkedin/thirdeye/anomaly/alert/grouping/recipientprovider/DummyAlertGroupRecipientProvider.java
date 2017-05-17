package com.linkedin.thirdeye.anomaly.alert.grouping.recipientprovider;

import com.linkedin.thirdeye.api.DimensionMap;

public class DummyAlertGroupRecipientProvider extends BaseAlertGroupRecipientProvider {
  @Override
  public String getAlertGroupRecipients(DimensionMap dimensions) {
    return EMPTY_RECIPIENTS;
  }
}
