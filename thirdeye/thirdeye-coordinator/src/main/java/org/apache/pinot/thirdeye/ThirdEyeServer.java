package org.apache.pinot.thirdeye;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.federecio.dropwizard.swagger.SwaggerBundle;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import org.apache.pinot.thirdeye.resources.RootResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThirdEyeServer extends Application<ThirdEyeServerConfiguration> {

  private static final Logger log = LoggerFactory.getLogger(ThirdEyeServer.class);

  public static void main(String[] args) throws Exception {
    RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
    log.info(String.format("JVM arguments: %s", runtimeMxBean.getInputArguments()));

    new ThirdEyeServer().run(args);
  }

  @Override
  public void initialize(final Bootstrap<ThirdEyeServerConfiguration> bootstrap) {
    bootstrap.addBundle(new SwaggerBundle<ThirdEyeServerConfiguration>() {
      @Override
      protected SwaggerBundleConfiguration getSwaggerBundleConfiguration(
          ThirdEyeServerConfiguration configuration) {
        return configuration.getSwaggerBundleConfiguration();
      }
    });
  }

  @Override
  public void run(final ThirdEyeServerConfiguration configuration, final Environment environment) {

    final Injector injector = Guice.createInjector(
        new ThirdEyeServerModule(configuration,
            environment.metrics()));

    environment.jersey().register(injector.getInstance(RootResource.class));
  }
}
