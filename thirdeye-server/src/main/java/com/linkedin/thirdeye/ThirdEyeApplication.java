package com.linkedin.thirdeye;

import static com.linkedin.thirdeye.ThirdEyeConstants.*;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.impl.StarTreeManagerImpl;
import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.File;
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
  public void run(Config config, Environment environment) throws Exception
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

    environment.admin().addTask(new ThirdEyeRestoreTask(manager, new File(config.getDataRoot())));
  }

  public static class Config extends Configuration
  {
    @NotEmpty
    private String dataRoot;

    @JsonProperty
    public String getDataRoot()
    {
      return dataRoot;
    }

    @JsonProperty
    public void setDataRoot(String dataRoot)
    {
      this.dataRoot = dataRoot;
    }
  }

  public static void main(String[] args) throws Exception
  {
    new ThirdEyeApplication().run(args);
  }
}
