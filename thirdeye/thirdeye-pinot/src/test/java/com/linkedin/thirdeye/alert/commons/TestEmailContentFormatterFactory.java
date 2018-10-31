package com.linkedin.thirdeye.alert.commons;

import com.linkedin.thirdeye.alert.content.EmailContentFormatter;
import com.linkedin.thirdeye.alert.content.HierarchicalAnomaliesEmailContentFormatter;
import com.linkedin.thirdeye.alert.content.MultipleAnomaliesEmailContentFormatter;
import org.junit.Assert;
import org.testng.annotations.Test;


public class TestEmailContentFormatterFactory {

  @Test
  public void testGetEmailContentFormatter() throws Exception{
    EmailContentFormatter emailContentFormatter;
    emailContentFormatter = EmailContentFormatterFactory.fromClassName("MultipleAnomaliesEmailContentFormatter");
    Assert.assertNotNull(emailContentFormatter);
    Assert.assertTrue(emailContentFormatter instanceof MultipleAnomaliesEmailContentFormatter);

    emailContentFormatter = EmailContentFormatterFactory.fromClassName("HierarchicalAnomaliesEmailContentFormatter");
    Assert.assertNotNull(emailContentFormatter);
    Assert.assertTrue(emailContentFormatter instanceof HierarchicalAnomaliesEmailContentFormatter);
  }
}
