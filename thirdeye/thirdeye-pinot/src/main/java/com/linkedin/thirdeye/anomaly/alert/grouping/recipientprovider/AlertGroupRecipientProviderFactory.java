package com.linkedin.thirdeye.anomaly.alert.grouping.recipientprovider;

import java.util.Collections;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

public class AlertGroupRecipientProviderFactory {
  public static final String GROUP_RECIPIENT_PROVIDER_TYPE_KEY = "type";
  private static final AlertGroupRecipientProvider DUMMY_ALERT_GROUP_RECIPIENT_PROVIDER =
      new DummyAlertGroupRecipientProvider();

  public enum GroupRecipientProviderType {
    DUMMY, DIMENSIONAL
  }

  public static AlertGroupRecipientProvider fromSpec(Map<String, String> spec) {
    if (MapUtils.isEmpty(spec)) {
      spec = Collections.emptyMap();
    }
    AlertGroupRecipientProvider alertGroupRecipientProvider =
        fromStringType(spec.get(GROUP_RECIPIENT_PROVIDER_TYPE_KEY));
    alertGroupRecipientProvider.setParameters(spec);

    return alertGroupRecipientProvider;
  }

  private static AlertGroupRecipientProvider fromStringType(String type) {
    if (StringUtils.isBlank(type)) { // backward compatibility
      return DUMMY_ALERT_GROUP_RECIPIENT_PROVIDER;
    }

    AlertGroupRecipientProviderFactory.GroupRecipientProviderType providerType = GroupRecipientProviderType.DUMMY;
    for (GroupRecipientProviderType enumProviderType : GroupRecipientProviderType.values()) {
      if (enumProviderType.name().compareToIgnoreCase(type) == 0) {
        providerType = enumProviderType;
        break;
      }
    }

    switch (providerType) {
    case DUMMY: // speed optimization for most use cases
      return DUMMY_ALERT_GROUP_RECIPIENT_PROVIDER;
    case DIMENSIONAL:
      return new DimensionalAlertGroupRecipientProvider();
    default:
      return DUMMY_ALERT_GROUP_RECIPIENT_PROVIDER;
    }
  }
}
