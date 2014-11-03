package com.linkedin.thirdeye;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.impl.StarTreeManagerImpl;
import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.net.URI;
import java.util.concurrent.ExecutorService;

public class ThirdEyeApplication extends Application<ThirdEyeApplication.Config>
{
  public static final String NAME = "thirdeye";
  public static final String BETWEEN = "__BETWEEN__";
  public static final String IN = "__IN__";
  public static final String TIME_SEPARATOR  = ",";

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

    final StarTreeManager starTreeManager = new StarTreeManagerImpl(executorService);

    final ThirdEyeMetricsResource metricsResource = new ThirdEyeMetricsResource(starTreeManager);
    final ThirdEyeBootstrapResource bootstrapResource = new ThirdEyeBootstrapResource(starTreeManager);
    final ThirdEyeConfigResource configResource = new ThirdEyeConfigResource(starTreeManager, config.getMaxRecordStoreEntries(), URI.create(config.getRootUri()));
    final ThirdEyeDimensionsResource dimensionsResource = new ThirdEyeDimensionsResource(starTreeManager);
    final ThirdEyeCollectionsResource collectionsResource = new ThirdEyeCollectionsResource(starTreeManager);

    final ThirdEyeHealthCheck healthCheck = new ThirdEyeHealthCheck();

    environment.healthChecks().register(NAME, healthCheck);

    environment.jersey().register(metricsResource);
    environment.jersey().register(bootstrapResource);
    environment.jersey().register(configResource);
    environment.jersey().register(dimensionsResource);
    environment.jersey().register(collectionsResource);
  }

  public static class Config extends Configuration
  {
    @NotEmpty
    private String rootUri;

    @NotNull
    private int maxRecordStoreEntries;

    @JsonProperty
    public String getRootUri()
    {
      return rootUri;
    }

    public void setRootUri(String rootUri)
    {
      this.rootUri = rootUri;
    }

    @JsonProperty
    public int getMaxRecordStoreEntries()
    {
      return maxRecordStoreEntries;
    }

    public void setMaxRecordStoreEntries(int maxRecordStoreEntries)
    {
      this.maxRecordStoreEntries = maxRecordStoreEntries;
    }
  }

  public static void main(String[] args) throws Exception
  {
    new ThirdEyeApplication().run(args);
  }
}
