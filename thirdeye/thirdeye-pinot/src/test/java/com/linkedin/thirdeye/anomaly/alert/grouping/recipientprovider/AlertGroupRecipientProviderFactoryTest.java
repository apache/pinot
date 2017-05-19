package com.linkedin.thirdeye.anomaly.alert.grouping.recipientprovider;

import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AlertGroupRecipientProviderFactoryTest {
  @Test
  public void testFromSpecNull() throws Exception {
    AlertGroupRecipientProviderFactory alertGroupRecipientProviderFactory = new AlertGroupRecipientProviderFactory();
    AlertGroupRecipientProvider recipientProvider = alertGroupRecipientProviderFactory.fromSpec(null);
    Assert.assertEquals(recipientProvider.getClass(), DummyAlertGroupRecipientProvider.class);
  }

  @Test
  public void testDimensionalAlertGroupRecipientProviderCreation() {
    AlertGroupRecipientProviderFactory alertGroupRecipientProviderFactory = new AlertGroupRecipientProviderFactory();
    Map<String, String> spec = new HashMap<>();
    spec.put(AlertGroupRecipientProviderFactory.GROUP_RECIPIENT_PROVIDER_TYPE_KEY, "diMenSionAL");
    AlertGroupRecipientProvider alertGrouper = alertGroupRecipientProviderFactory.fromSpec(spec);
    Assert.assertEquals(alertGrouper.getClass(), DimensionalAlertGroupRecipientProvider.class);
  }
}
