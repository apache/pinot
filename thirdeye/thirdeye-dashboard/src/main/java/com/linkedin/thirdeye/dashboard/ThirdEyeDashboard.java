package com.linkedin.thirdeye.dashboard;

import java.io.File;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.client.CollectionMapThirdEyeClient;
import com.linkedin.thirdeye.dashboard.resources.CollectionConfigResource;
import com.linkedin.thirdeye.dashboard.resources.ContributorDataProvider;
import com.linkedin.thirdeye.dashboard.resources.CustomDashboardResource;
import com.linkedin.thirdeye.dashboard.resources.DashboardConfigResource;
import com.linkedin.thirdeye.dashboard.resources.DashboardResource;
import com.linkedin.thirdeye.dashboard.resources.FlotTimeSeriesResource;
import com.linkedin.thirdeye.dashboard.resources.FunnelsDataProvider;
import com.linkedin.thirdeye.dashboard.resources.MetadataResource;
import com.linkedin.thirdeye.dashboard.task.ClearCachesTask;
import com.linkedin.thirdeye.dashboard.util.ConfigCache;
import com.linkedin.thirdeye.dashboard.util.DataCache;
import com.linkedin.thirdeye.dashboard.util.QueryCache;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.views.ViewBundle;

public class ThirdEyeDashboard extends Application<ThirdEyeDashboardConfiguration> {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeDashboard.class);

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

    LOG.info("running the Dashboard application with configs, {}", config.toString());

    // TODO upgrade dropwizard version so that this returns a CloseableHttpClient that can in turn
    // be passed to DefaultThirdEyeClient via new constructors
    // opt: also pass in object mapper, environment.getObjectMapper()
    // final HttpClient httpClient =
    // new HttpClientBuilder(environment).using(config.getHttpClient()).build(getName());

    ExecutorService queryExecutor =
        environment.lifecycle().executorService("query_executor").build();

    String clientConfigFilePath = config.getClientConfigRoot();
    CollectionMapThirdEyeClient clientMap =
        CollectionMapThirdEyeClient.fromFolder(clientConfigFilePath);
    DataCache dataCache = new DataCache(clientMap);
    QueryCache queryCache = new QueryCache(clientMap, queryExecutor);

    ConfigCache configCache = new ConfigCache();
    CustomDashboardResource customDashboardResource = null;
    if (config.getCustomDashboardRoot() != null) {
      File customDashboardDir = new File(config.getCustomDashboardRoot());
      configCache.setCustomDashboardRoot(customDashboardDir);
      customDashboardResource =
          new CustomDashboardResource(customDashboardDir, queryCache, dataCache, configCache);
      environment.jersey().register(customDashboardResource);
    }

    FunnelsDataProvider funnelsResource = null;
    if (config.getFunnelConfigRoot() != null) {
      funnelsResource =
          new FunnelsDataProvider(new File(config.getFunnelConfigRoot()), queryCache, dataCache);
      environment.jersey().register(funnelsResource);
    }

    if (config.getCollectionConfigRoot() != null) {
      File collectionConfigDir = new File(config.getCollectionConfigRoot());
      configCache.setCollectionConfigRoot(collectionConfigDir);
      CollectionConfigResource collectionConfigResource =
          new CollectionConfigResource(collectionConfigDir, configCache);
      environment.jersey().register(collectionConfigResource);
    }

    DashboardConfigResource dashboardConfigResource =
        new DashboardConfigResource(dataCache, queryCache, clientMap, clientConfigFilePath,
            funnelsResource, environment.getObjectMapper());
    environment.jersey().register(dashboardConfigResource);

    ContributorDataProvider contributorResource =
        new ContributorDataProvider(queryCache, environment.getObjectMapper());
    environment.jersey().register(contributorResource);

    environment.jersey()
        .register(new DashboardResource(dataCache, config.getFeedbackEmailAddress(), queryCache,
            environment.getObjectMapper(), customDashboardResource, configCache, funnelsResource,
            contributorResource, dashboardConfigResource));

    environment.jersey().register(new FlotTimeSeriesResource(dataCache, queryCache,
        environment.getObjectMapper(), configCache, config.getAnomalyDatabaseConfig()));

    environment.jersey().register(new MetadataResource(dataCache));

    environment.admin().addTask(new ClearCachesTask(dataCache, queryCache, configCache));
  }

  public static void main(String[] args) throws Exception {
    new ThirdEyeDashboard().run(args);
  }

}
