package org.apache.pinot.common.utils.helix;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.InstanceConfig;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HelixHelperTest {
  @Test
  public void testUpdateInstanceHostNamePort() {
    HelixManager mockedManager = Mockito.mock(HelixManager.class);
    HelixDataAccessor mockedAccessor = Mockito.mock(HelixDataAccessor.class);
    HelixAdmin mockedAdmin = Mockito.mock(HelixAdmin.class);
    Mockito.when(mockedManager.getHelixDataAccessor()).thenReturn(mockedAccessor);
    Mockito.when(mockedManager.getClusterManagmentTool()).thenReturn(mockedAdmin);
    final String clusterName = "amazing_cluster";
    final String instanceId = "some_unique_id";
    InstanceConfig targetConfig = new InstanceConfig(instanceId);
    Mockito.when(mockedAdmin.getInstanceConfig(clusterName, instanceId)).thenReturn(targetConfig);
    Mockito.when(mockedAccessor.keyBuilder()).thenReturn(new PropertyKey.Builder(clusterName));
    Mockito.when(mockedAccessor.setProperty(Mockito.any(PropertyKey.class), Mockito.eq(targetConfig))).thenReturn(true);
    HelixHelper.updateInstanceHostNamePort(mockedManager, clusterName, instanceId, "strange.host.com", 234);
    Mockito.verify(mockedAccessor).setProperty(Mockito.any(PropertyKey.class), Mockito.eq(targetConfig));
    Assert.assertEquals("strange.host.com", targetConfig.getHostName());
    Assert.assertEquals("234", targetConfig.getPort());

  }
}
