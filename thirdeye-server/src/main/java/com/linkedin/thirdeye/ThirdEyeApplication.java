package com.linkedin.thirdeye;

import static com.linkedin.thirdeye.ThirdEyeConstants.*;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.healthcheck.DefaultHealthCheck;
import com.linkedin.thirdeye.impl.StarTreeManagerImpl;
import com.linkedin.thirdeye.resource.CollectionsResource;
import com.linkedin.thirdeye.resource.DashboardResource;
import com.linkedin.thirdeye.resource.DimensionsResource;
import com.linkedin.thirdeye.resource.HeatMapResource;
import com.linkedin.thirdeye.resource.MetricsResource;
import com.linkedin.thirdeye.resource.PingResource;
import com.linkedin.thirdeye.resource.TimeSeriesResource;
import com.linkedin.thirdeye.task.DumpTreeTask;
import com.linkedin.thirdeye.task.RestoreTask;
import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.views.ViewBundle;
import org.apache.commons.io.FileUtils;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.File;

public class ThirdEyeApplication extends Application<ThirdEyeApplication.Config>
{
  @Override
  public String getName()
  {
    return NAME;
  }

  @Override
  public void initialize(Bootstrap<Config> bootstrap)
  {
    bootstrap.addBundle(new ViewBundle());
    bootstrap.addBundle(new AssetsBundle("/assets/stylesheets", "/assets/stylesheets", null, "stylesheets"));
    bootstrap.addBundle(new AssetsBundle("/assets/javascripts", "/assets/javascripts", null, "javascripts"));
    bootstrap.addBundle(new AssetsBundle("/assets/images", "/assets/images", null, "images"));
  }

  @Override
  public void run(Config config, Environment environment) throws Exception
  {
    File rootDir = new File(config.getRootDir());

    if (!rootDir.exists())
    {
      FileUtils.forceMkdir(rootDir);
    }

    StarTreeManager starTreeManager = new StarTreeManagerImpl();

    environment.healthChecks().register(NAME, new DefaultHealthCheck());

    environment.jersey().register(new MetricsResource(starTreeManager));
    environment.jersey().register(new DimensionsResource(starTreeManager));
    environment.jersey().register(new CollectionsResource(starTreeManager, rootDir));
    environment.jersey().register(new TimeSeriesResource(starTreeManager));
    environment.jersey().register(new HeatMapResource(starTreeManager));
    environment.jersey().register(new PingResource());

    environment.jersey().register(new DashboardResource(starTreeManager));

    environment.admin().addTask(new RestoreTask(starTreeManager, rootDir));
    environment.admin().addTask(new DumpTreeTask(starTreeManager));

    environment.lifecycle().addLifeCycleListener(new ThirdEyeLifeCycleListener(starTreeManager));

    if (config.isAutoRestore())
    {
      String[] collections = rootDir.list();
      if (collections != null)
      {
        for (String collection : collections)
        {
          starTreeManager.restore(rootDir, collection);
          starTreeManager.open(collection);
        }
      }
    }
  }

  public static class Config extends Configuration
  {
    @NotEmpty
    private String rootDir;

    private boolean autoRestore;

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

    public boolean isAutoRestore()
    {
      return autoRestore;
    }

    public void setAutoRestore(boolean autoRestore)
    {
      this.autoRestore = autoRestore;
    }
  }

  public static void main(String[] args) throws Exception
  {
    new ThirdEyeApplication().run(args);
  }
}
