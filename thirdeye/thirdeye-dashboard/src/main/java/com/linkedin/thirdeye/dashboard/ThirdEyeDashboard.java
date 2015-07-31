package com.linkedin.thirdeye.dashboard;

import com.linkedin.thirdeye.dashboard.resources.*;
import com.linkedin.thirdeye.dashboard.task.ClearCachesTask;
import com.linkedin.thirdeye.dashboard.util.DataCache;
import com.linkedin.thirdeye.dashboard.util.QueryCache;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.client.HttpClientBuilder;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.views.ViewBundle;
import org.apache.http.client.HttpClient;

import java.io.File;
import java.util.concurrent.ExecutorService;

public class ThirdEyeDashboard extends Application<ThirdEyeDashboardConfiguration> {
  @Override
  public String getName() {
    return "thirdeye-dashboard";
  }

  @Override
  public void initialize(Bootstrap<ThirdEyeDashboardConfiguration> bootstrap) {
    bootstrap.addBundle(new ViewBundle());
    bootstrap.addBundle(new AssetsBundle("/assets/css", "/assets/css", null, "css"));
    bootstrap.addBundle(new AssetsBundle("/assets/js", "/assets/js", null, "js"));
    bootstrap.addBundle(new AssetsBundle("/assets/img", "/assets/img", null, "img"));
  }

  @Override
  public void run(ThirdEyeDashboardConfiguration config, Environment environment) throws Exception {
    final HttpClient httpClient =
        new HttpClientBuilder(environment)
            .using(config.getHttpClient())
            .build(getName());

    ExecutorService queryExecutor = environment.lifecycle().executorService("query_executor").build();

    DataCache dataCache = new DataCache(httpClient, environment.getObjectMapper());

    QueryCache queryCache = new QueryCache(httpClient, environment.getObjectMapper(), queryExecutor);

    CustomDashboardResource customDashboardResource = null;
    if (config.getCustomDashboardRoot() != null) {
      File customDashboardDir = new File(config.getCustomDashboardRoot());
      customDashboardResource = new CustomDashboardResource(customDashboardDir, config.getServerUri(), queryCache, dataCache);
      environment.jersey().register(customDashboardResource);
    }

    environment.jersey().register(new DashboardResource(
        config.getServerUri(),
        dataCache,
        config.getFeedbackEmailAddress(),
        queryCache,
        environment.getObjectMapper(),
        customDashboardResource));

    environment.jersey().register(new FlotTimeSeriesResource(
        config.getServerUri(),
        dataCache,
        queryCache,
        environment.getObjectMapper()));

    environment.jersey().register(new MetadataResource(config.getServerUri(), dataCache));

    environment.admin().addTask(new ClearCachesTask(dataCache, queryCache));
  }

  public static void main(String[] args) throws Exception {
    new ThirdEyeDashboard().run(args);
  }
}
