package com.linkedin.thirdeye;

import static com.linkedin.thirdeye.ThirdEyeConstants.*;

import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.impl.StarTreeManagerImpl;
import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import java.util.concurrent.ExecutorService;

public class ThirdEyeApplication extends Application<ThirdEyeApplication.Config>
{
  @Override
  public String getName()
  {
    return NAME;
  }

  @Override
  public void initialize(Bootstrap<Config> thirdEyeConfigurationBootstrap)
  {
    // Do nothing
  }

  @Override
  public void run(Config thirdEyeConfiguration, Environment environment) throws Exception
  {
    ExecutorService executorService
            = environment.lifecycle()
                         .executorService("starTreeManager")
                         .minThreads(Runtime.getRuntime().availableProcessors())
                         .maxThreads(Runtime.getRuntime().availableProcessors())
                         .build();

    StarTreeManager manager = new StarTreeManagerImpl(executorService);

    environment.jersey().register(new ThirdEyeMetricsResource(manager));
    environment.jersey().register(new ThirdEyeDimensionsResource(manager));
    environment.jersey().register(new ThirdEyeCollectionsResource(manager));

    environment.healthChecks().register(NAME, new ThirdEyeHealthCheck());

    environment.admin().addTask(new ThirdEyeRestoreTask(manager));
  }

  public static class Config extends Configuration
  {
    // TODO
  }

  public static void main(String[] args) throws Exception
  {
    new ThirdEyeApplication().run(args);
  }
}
