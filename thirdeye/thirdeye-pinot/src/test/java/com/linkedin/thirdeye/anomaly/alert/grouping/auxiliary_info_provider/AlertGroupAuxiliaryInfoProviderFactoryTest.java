package com.linkedin.thirdeye.anomaly.alert.grouping.auxiliary_info_provider;

import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AlertGroupAuxiliaryInfoProviderFactoryTest {
  @Test
  public void testFromSpecNull() throws Exception {
    AlertGroupRecipientProviderFactory alertGroupRecipientProviderFactory = new AlertGroupRecipientProviderFactory();
    AlertGroupAuxiliaryInfoProvider recipientProvider = alertGroupRecipientProviderFactory.fromSpec(null);
    Assert.assertEquals(recipientProvider.getClass(), DummyAlertGroupAuxiliaryInfoProvider.class);
  }

  @Test
  public void testDimensionalAlertGroupRecipientProviderCreation() {
    AlertGroupRecipientProviderFactory alertGroupRecipientProviderFactory = new AlertGroupRecipientProviderFactory();
    Map<String, String> spec = new HashMap<>();
    spec.put(AlertGroupRecipientProviderFactory.GROUP_RECIPIENT_PROVIDER_TYPE_KEY, "diMenSionAL");
    AlertGroupAuxiliaryInfoProvider alertGrouper = alertGroupRecipientProviderFactory.fromSpec(spec);
    Assert.assertEquals(alertGrouper.getClass(), DimensionalAlertGroupAuxiliaryRecipientProvider.class);
  }
}
