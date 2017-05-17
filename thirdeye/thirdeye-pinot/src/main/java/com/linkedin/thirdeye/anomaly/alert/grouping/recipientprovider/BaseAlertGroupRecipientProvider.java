package com.linkedin.thirdeye.anomaly.alert.grouping.recipientprovider;

import java.util.Collections;
import java.util.Map;

public abstract class BaseAlertGroupRecipientProvider implements AlertGroupRecipientProvider {
  public static String EMPTY_RECIPIENTS = "";
  public static String RECIPIENTS_SEPARATOR = ",";

  Map<String, String> props = Collections.emptyMap();

  @Override
  public void setParameters(Map<String, String> props) {
    this.props = props;
  }
}
