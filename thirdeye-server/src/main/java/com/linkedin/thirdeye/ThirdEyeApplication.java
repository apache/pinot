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
import com.linkedin.thirdeye.task.ThirdEyePartitionTask;
import com.linkedin.thirdeye.task.ThirdEyeRestoreTask;
import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.api.id.StateModelDefId;
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

    StarTreeManager starTreeManager = new StarTreeManagerImpl(executorService);

    File rootDir = new File(config.getRootDir());
    File tmpDir = new File(config.getTmpDir());

    HelixManager helixManager = null;
    if (config.isDistributed())
    {
      helixManager
              = HelixManagerFactory.getZKHelixManager(config.getClusterName(),
                                                      config.getInstanceName(),
                                                      InstanceType.PARTICIPANT,
                                                      config.getZkAddress());

      helixManager.getStateMachineEngine()
                  .registerStateModelFactory(StateModelDefId.OnlineOffline,
                                             new ThirdEyeTransitionHandlerFactory(starTreeManager, rootDir));
    }

    environment.jersey().register(new ThirdEyeMetricsResource(starTreeManager));
    environment.jersey().register(new ThirdEyeDimensionsResource(starTreeManager));
    environment.jersey().register(new ThirdEyeCollectionsResource(starTreeManager));
    environment.jersey().register(new ThirdEyeTimeSeriesResource(starTreeManager));

    environment.healthChecks().register(NAME, new ThirdEyeHealthCheck());

    environment.admin().addTask(new ThirdEyeRestoreTask(starTreeManager, rootDir));
    environment.admin().addTask(new ThirdEyeCreateTask(starTreeManager));
    environment.admin().addTask(new ThirdEyeDumpTreeTask(starTreeManager));
    environment.admin().addTask(new ThirdEyeDumpBufferTask(starTreeManager));
    environment.admin().addTask(new ThirdEyeBulkLoadTask(executorService, starTreeManager, rootDir, tmpDir));
    environment.admin().addTask(new ThirdEyeBootstrapTask(new File(config.getRootDir())));

    if (helixManager != null)
    {
      environment.admin().addTask(new ThirdEyePartitionTask(helixManager, rootDir));
    }

    environment.lifecycle().addLifeCycleListener(new ThirdEyeLifeCycleListener(helixManager, starTreeManager));
  }

  public static class Config extends Configuration
  {
    @NotEmpty
    private String rootDir;

    @NotEmpty
    private String tmpDir;

    // Helix
    private boolean distributed;
    private String clusterName;
    private String instanceName;
    private String zkAddress;

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
    public String getInstanceName()
    {
      return instanceName;
    }

    @JsonProperty
    public void setInstanceName(String instanceName)
    {
      this.instanceName = instanceName;
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
  }

  public static void main(String[] args) throws Exception
  {
    new ThirdEyeApplication().run(args);
  }
}
