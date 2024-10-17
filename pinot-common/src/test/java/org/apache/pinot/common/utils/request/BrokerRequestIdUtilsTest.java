package org.apache.pinot.common.utils.request;

import org.testng.Assert;
import org.testng.annotations.Test;


public class BrokerRequestIdUtilsTest {
  @Test
  public void testGetOfflineRequestId() {
    Assert.assertEquals(BrokerRequestIdUtils.getOfflineRequestId(123), 120);
  }

  @Test
  public void testGetRealtimeRequestId() {
    Assert.assertEquals(BrokerRequestIdUtils.getRealtimeRequestId(123), 121);
  }

  @Test
  public void testGetCanonicalRequestId() {
    Assert.assertEquals(BrokerRequestIdUtils.getCanonicalRequestId(123), 120);
  }
}
