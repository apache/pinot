package com.linkedin.thirdeye;

import static com.linkedin.thirdeye.ThirdEyeConstants.*;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.healthcheck.ThirdEyeHealthCheck;
import com.linkedin.thirdeye.impl.StarTreeManagerImpl;
import com.linkedin.thirdeye.resource.ThirdEyeCollectionsResource;
import com.linkedin.thirdeye.resource.ThirdEyeDimensionsResource;
import com.linkedin.thirdeye.resource.ThirdEyeMetricsResource;
import com.linkedin.thirdeye.resource.ThirdEyeTimeSeriesResource;
import com.linkedin.thirdeye.task.ThirdEyeBootstrapTask;
import com.linkedin.thirdeye.task.ThirdEyeBulkLoadTask;
import com.linkedin.thirdeye.task.ThirdEyeCreateTask;
import com.linkedin.thirdeye.task.ThirdEyeDumpBufferTask;
import com.linkedin.thirdeye.task.ThirdEyeDumpTreeTask;
import com.linkedin.thirdeye.task.ThirdEyeRestoreTask;
import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.eclipse.jetty.util.component.LifeCycle;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.File;
import java.io.IOException;
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

    final StarTreeManager manager = new StarTreeManagerImpl(executorService);

    environment.jersey().register(new ThirdEyeMetricsResource(manager));
    environment.jersey().register(new ThirdEyeDimensionsResource(manager));
    environment.jersey().register(new ThirdEyeCollectionsResource(manager));
    environment.jersey().register(new ThirdEyeTimeSeriesResource(manager));

    environment.healthChecks().register(NAME, new ThirdEyeHealthCheck());

    environment.admin().addTask(new ThirdEyeRestoreTask(manager, new File(config.getRootDir())));
    environment.admin().addTask(new ThirdEyeCreateTask(manager));
    environment.admin().addTask(new ThirdEyeDumpTreeTask(manager));
    environment.admin().addTask(new ThirdEyeDumpBufferTask(manager));
    environment.admin().addTask(new ThirdEyeBulkLoadTask(executorService, manager, new File(config.getRootDir()), new File(config.getTmpDir())));
    environment.admin().addTask(new ThirdEyeBootstrapTask(new File(config.getRootDir())));

    environment.lifecycle().addLifeCycleListener(new ThirdEyeLifeCycleListener(manager));
  }

  public static class Config extends Configuration
  {
    @NotEmpty
    private String rootDir;

    @NotEmpty
    private String tmpDir;

    @JsonProperty
    public String getRootDir()
    {
      return rootDir;
    }

    @JsonProperty
    public void setRootDir(String rootDir)
    {
      this.rootDir = rootDir;
    }

    @JsonProperty
    public String getTmpDir()
    {
      return tmpDir;
    }

    @JsonProperty
    public void setTmpDir(String tmpDir)
    {
      this.tmpDir = tmpDir;
    }
  }

  public static void main(String[] args) throws Exception
  {
    new ThirdEyeApplication().run(args);
  }
}
