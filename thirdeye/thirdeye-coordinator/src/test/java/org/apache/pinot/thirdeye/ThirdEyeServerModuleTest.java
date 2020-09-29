package org.apache.pinot.thirdeye;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import com.codahale.metrics.MetricRegistry;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.pinot.thirdeye.resources.RootResource;
import org.testng.annotations.Test;

public class ThirdEyeServerModuleTest {

  @Test
  public void testRootResourceInjection() {
    final Injector injector = Guice.createInjector(new ThirdEyeServerModule(
       mock(ThirdEyeServerConfiguration.class),
       mock(MetricRegistry.class)
    ));
    assertThat(injector.getInstance(RootResource.class)).isNotNull();
  }
}