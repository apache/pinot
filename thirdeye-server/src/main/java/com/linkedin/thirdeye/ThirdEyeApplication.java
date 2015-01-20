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
import com.linkedin.thirdeye.task.ThirdEyeDumpTreeTask;
import com.linkedin.thirdeye.task.ThirdEyeRestoreTask;
import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.jetty.ConnectorFactory;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.server.DefaultServerFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

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
    File rootDir = new File(config.getRootDir());

    StarTreeManager starTreeManager = new StarTreeManagerImpl();

    environment.healthChecks().register(NAME, new ThirdEyeHealthCheck());

    environment.jersey().register(new ThirdEyeMetricsResource(starTreeManager));
    environment.jersey().register(new ThirdEyeDimensionsResource(starTreeManager));
    environment.jersey().register(new ThirdEyeCollectionsResource(starTreeManager));
    environment.jersey().register(new ThirdEyeTimeSeriesResource(starTreeManager));

    environment.admin().addTask(new ThirdEyeRestoreTask(starTreeManager, rootDir));
    environment.admin().addTask(new ThirdEyeDumpTreeTask(starTreeManager));

    environment.lifecycle().addLifeCycleListener(new ThirdEyeLifeCycleListener(starTreeManager));
  }

  private static InetSocketAddress getLocalHost(Config config) throws UnknownHostException
  {
    ConnectorFactory connectorFactory
            = ((DefaultServerFactory) config.getServerFactory()).getApplicationConnectors().get(0);
    int port;
    if (connectorFactory instanceof HttpConnectorFactory)
    {
      port = ((HttpConnectorFactory) connectorFactory).getPort();
    }
    else
    {
      throw new IllegalArgumentException("Unrecognized connector factory " + connectorFactory);
    }
    return new InetSocketAddress(InetAddress.getLocalHost().getHostName(), port);
  }

  public static class Config extends Configuration
  {
    @NotEmpty
    private String rootDir;

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
  }

  public static void main(String[] args) throws Exception
  {
    new ThirdEyeApplication().run(args);
  }
}
