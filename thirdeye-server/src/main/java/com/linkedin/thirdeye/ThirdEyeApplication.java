package com.linkedin.thirdeye;

import static com.linkedin.thirdeye.ThirdEyeConstants.*;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.cluster.ThirdEyeTransitionHandlerFactory;
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
import com.linkedin.thirdeye.task.ThirdEyeRebalanceTask;
import com.linkedin.thirdeye.task.ThirdEyeRestoreTask;
import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.client.HttpClientConfiguration;
import io.dropwizard.jetty.ConnectorFactory;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.server.DefaultServerFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.api.id.StateModelDefId;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
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
    File rootDir = new File(config.getRootDir());

    ExecutorService executorService
            = environment.lifecycle()
                         .executorService("starTreeManager")
                         .minThreads(Runtime.getRuntime().availableProcessors())
                         .maxThreads(Runtime.getRuntime().availableProcessors())
                         .build();

    StarTreeManager starTreeManager = new StarTreeManagerImpl(executorService, rootDir);

    URI archiveSource = config.getArchiveSource() == null ? null : URI.create(config.getArchiveSource());

    HelixManager helixManager = null;
    if (config.isDistributed())
    {
      InetSocketAddress localHost = getLocalHost(config);
      String instanceName = String.format("%s_%d", localHost.getHostName(), localHost.getPort());

      helixManager
              = HelixManagerFactory.getZKHelixManager(config.getClusterName(),
                                                      instanceName,
                                                      InstanceType.PARTICIPANT,
                                                      config.getZkAddress());

      helixManager.getStateMachineEngine()
                  .registerStateModelFactory(StateModelDefId.OnlineOffline,
                                             new ThirdEyeTransitionHandlerFactory(starTreeManager,
                                                                                  archiveSource,
                                                                                  rootDir));

      environment.admin().addTask(new ThirdEyeRebalanceTask(config.getClusterName(), helixManager.getClusterManagmentTool()));
    }

    environment.healthChecks().register(NAME, new ThirdEyeHealthCheck());

    environment.jersey().register(new ThirdEyeMetricsResource(starTreeManager));
    environment.jersey().register(new ThirdEyeDimensionsResource(starTreeManager));
    environment.jersey().register(new ThirdEyeCollectionsResource(starTreeManager, helixManager));
    environment.jersey().register(new ThirdEyeTimeSeriesResource(starTreeManager));

    environment.admin().addTask(new ThirdEyeRestoreTask(starTreeManager, rootDir));
    environment.admin().addTask(new ThirdEyeCreateTask(starTreeManager));
    environment.admin().addTask(new ThirdEyeDumpTreeTask(starTreeManager));
    environment.admin().addTask(new ThirdEyeDumpBufferTask(starTreeManager));
    environment.admin().addTask(new ThirdEyeBulkLoadTask(executorService, starTreeManager, rootDir));
    environment.admin().addTask(new ThirdEyeBootstrapTask(new File(config.getRootDir())));

    environment.lifecycle().addLifeCycleListener(new ThirdEyeLifeCycleListener(helixManager, starTreeManager));
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

    // Helix (optional)
    private boolean distributed;
    private String clusterName;
    private String zkAddress;

    // Http client configuration (optional)
    @Valid
    @NotNull
    @JsonProperty
    private HttpClientConfiguration httpClient = new HttpClientConfiguration();

    public HttpClientConfiguration getHttpClientConfiguration()
    {
      return httpClient;
    }

    private String archiveSource;

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
    public boolean isDistributed()
    {
      return distributed;
    }

    @JsonProperty
    public void setDistributed(boolean distributed)
    {
      this.distributed = distributed;
    }

    @JsonProperty
    public String getClusterName()
    {
      return clusterName;
    }

    @JsonProperty
    public void setClusterName(String clusterName)
    {
      this.clusterName = clusterName;
    }

    @JsonProperty
    public String getZkAddress()
    {
      return zkAddress;
    }

    @JsonProperty
    public void setZkAddress(String zkAddress)
    {
      this.zkAddress = zkAddress;
    }

    @JsonProperty
    public String getArchiveSource()
    {
      return archiveSource;
    }

    @JsonProperty
    public void setArchiveSource(String archiveSource)
    {
      this.archiveSource = archiveSource;
    }
  }

  public static void main(String[] args) throws Exception
  {
    new ThirdEyeApplication().run(args);
  }
}
