package com.linkedin.thirdeye.dashboard;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.linkedin.thirdeye.client.QueryCache;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClient;
import com.linkedin.thirdeye.dashboard.configs.AbstractConfigDAO;
import com.linkedin.thirdeye.dashboard.configs.DashboardConfig;
import com.linkedin.thirdeye.dashboard.configs.FileBasedConfigDAOFactory;
import com.linkedin.thirdeye.dashboard.resources.DashboardResource;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.views.ViewBundle;

public class ThirdEyeDashboard extends Application<ThirdEyeDashboardConfig> {
  public ThirdEyeDashboard() {

  }

  @Override
  public String getName() {
    return "thirdeye";
  }

  @Override
  public void initialize(Bootstrap<ThirdEyeDashboardConfig> bootstrap) {
    bootstrap.addBundle(new ViewBundle());
    bootstrap.addBundle(new HelperBundle());
    bootstrap.addBundle(new AssetsBundle("/assets", "/assets"));
    bootstrap.addBundle(new AssetsBundle("/assets/css", "/assets/css", null, "css"));
    bootstrap.addBundle(new AssetsBundle("/assets/js", "/assets/js", null, "js"));
    bootstrap.addBundle(new AssetsBundle("/assets/lib", "/assets/lib", null, "lib"));
    bootstrap.addBundle(new AssetsBundle("/assets/img", "/assets/img", null, "img"));
    bootstrap.addBundle(new AssetsBundle("/assets/data", "/assets/data", null, "data"));
  }

  @Override
  public void run(ThirdEyeDashboardConfig config, Environment env) throws Exception {
    ThirdEyeClient thirdEyeClient = createThirdEyeClient(config);
    ExecutorService queryExecutor = env.lifecycle().executorService("query_executor").build();
    QueryCache queryCache = createQueryCache(thirdEyeClient, queryExecutor);
    AbstractConfigDAO<DashboardConfig> configDAO = getDashboardConfig(config);

    env.jersey().register(new DashboardResource(queryCache, configDAO));
  }

  private QueryCache createQueryCache(ThirdEyeClient thirdEyeClient,
      ExecutorService queryExecutor) {
    QueryCache queryCache = new QueryCache(thirdEyeClient, Executors.newFixedThreadPool(10));
    return queryCache;
  }

  private ThirdEyeClient createThirdEyeClient(ThirdEyeDashboardConfig config) {
    return PinotThirdEyeClient.getDefaultTestClient(); // TODO make this
                                                // configurable - I'd recommend extending the
                                                // multiple-client implementation and retrieving
                                                // configurations from the central database
                                                // (CollectionMapThirdEyeClient)
  }

  private AbstractConfigDAO<DashboardConfig> getDashboardConfig(ThirdEyeDashboardConfig config) {
    String rootDirectory = "/tmp/config_root_dir";
    FileBasedConfigDAOFactory configDAOFactory = new FileBasedConfigDAOFactory(rootDirectory);
    AbstractConfigDAO<DashboardConfig> configDAO = configDAOFactory.getDashboardConfigDAO();
    return configDAO;
  }

  public static void main(String[] args) throws Exception {
    // TODO: Get config from a file
    new ThirdEyeDashboard().run(new String[] {
      "server"
    });
  }

}
