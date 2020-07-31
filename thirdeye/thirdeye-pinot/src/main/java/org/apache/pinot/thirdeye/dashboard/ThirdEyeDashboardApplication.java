/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.dashboard;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.cache.CacheBuilder;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.auth.Authenticator;
import org.apache.pinot.thirdeye.api.application.ApplicationResource;
import org.apache.pinot.thirdeye.auth.ThirdEyeCredentials;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.auth.ThirdEyeAuthFilter;
import org.apache.pinot.thirdeye.auth.ThirdEyeAuthenticatorDisabled;
import org.apache.pinot.thirdeye.auth.ThirdEyeLdapAuthenticator;
import org.apache.pinot.thirdeye.auth.ThirdEyePrincipal;
import org.apache.pinot.thirdeye.common.BaseThirdEyeApplication;
import org.apache.pinot.thirdeye.common.ThirdEyeSwaggerBundle;
import org.apache.pinot.thirdeye.dashboard.configs.AuthConfiguration;
import org.apache.pinot.thirdeye.dashboard.configs.ResourceConfiguration;
import org.apache.pinot.thirdeye.dashboard.resources.AdminResource;
import org.apache.pinot.thirdeye.dashboard.resources.AnomalyFlattenResource;
import org.apache.pinot.thirdeye.dashboard.resources.AnomalyResource;
import org.apache.pinot.thirdeye.dashboard.resources.AutoOnboardResource;
import org.apache.pinot.thirdeye.dashboard.resources.CacheResource;
import org.apache.pinot.thirdeye.dashboard.resources.CustomizedEventResource;
import org.apache.pinot.thirdeye.dashboard.resources.DashboardResource;
import org.apache.pinot.thirdeye.dashboard.resources.DatasetConfigResource;
import org.apache.pinot.thirdeye.dashboard.resources.v2.alerts.AlertResource;
import org.apache.pinot.thirdeye.dashboard.resources.EntityManagerResource;
import org.apache.pinot.thirdeye.dashboard.resources.EntityMappingResource;
import org.apache.pinot.thirdeye.dashboard.resources.MetricConfigResource;
import org.apache.pinot.thirdeye.dashboard.resources.OnboardDatasetMetricResource;
import org.apache.pinot.thirdeye.dashboard.resources.SummaryResource;
import org.apache.pinot.thirdeye.dashboard.resources.ThirdEyeResource;
import org.apache.pinot.thirdeye.dashboard.resources.v2.AnomaliesResource;
import org.apache.pinot.thirdeye.dashboard.resources.v2.AuthResource;
import org.apache.pinot.thirdeye.dashboard.resources.v2.ConfigResource;
import org.apache.pinot.thirdeye.dashboard.resources.v2.DataResource;
import org.apache.pinot.thirdeye.dashboard.resources.v2.DetectionAlertResource;
import org.apache.pinot.thirdeye.dashboard.resources.v2.RootCauseEntityFormatter;
import org.apache.pinot.thirdeye.dashboard.resources.v2.RootCauseMetricResource;
import org.apache.pinot.thirdeye.dashboard.resources.v2.RootCauseResource;
import org.apache.pinot.thirdeye.dashboard.resources.v2.RootCauseSessionResource;
import org.apache.pinot.thirdeye.api.user.dashboard.UserDashboardResource;
import org.apache.pinot.thirdeye.dashboard.resources.v2.rootcause.DefaultEntityFormatter;
import org.apache.pinot.thirdeye.dashboard.resources.v2.rootcause.FormatterLoader;
import org.apache.pinot.thirdeye.dataset.DatasetAutoOnboardResource;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.loader.AggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultAggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import org.apache.pinot.thirdeye.datasource.loader.TimeSeriesLoader;
import org.apache.pinot.thirdeye.datasource.sql.resources.SqlDataSourceResource;
import org.apache.pinot.thirdeye.detection.DetectionResource;
import org.apache.pinot.thirdeye.detection.DetectionConfigurationResource;
import org.apache.pinot.thirdeye.detection.yaml.YamlResource;
import org.apache.pinot.thirdeye.detector.email.filter.AlertFilterFactory;
import org.apache.pinot.thirdeye.detector.function.AnomalyFunctionFactory;
import org.apache.pinot.thirdeye.misc.HealthCheckResource;
import org.apache.pinot.thirdeye.model.download.ModelDownloaderManager;
import org.apache.pinot.thirdeye.rootcause.RCAFramework;
import org.apache.pinot.thirdeye.rootcause.impl.RCAFrameworkLoader;
import org.apache.pinot.thirdeye.tracking.RequestStatisticsLogger;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.auth.CachingAuthenticator;
import io.dropwizard.bundles.redirect.PathRedirect;
import io.dropwizard.bundles.redirect.RedirectBundle;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.views.ViewBundle;
import java.io.File;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The type Third eye dashboard application.
 */
public class ThirdEyeDashboardApplication
    extends BaseThirdEyeApplication<ThirdEyeDashboardConfiguration> {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeDashboardApplication.class);

  private RequestStatisticsLogger requestStatisticsLogger;
  private ModelDownloaderManager modelDownloaderManager;

  @Override
  public String getName() {
    return "Thirdeye Dashboard";
  }

  @SuppressWarnings("unchecked")
  @Override
  public void initialize(Bootstrap<ThirdEyeDashboardConfiguration> bootstrap) {
    bootstrap.addBundle(new ViewBundle());
    bootstrap.addBundle(new HelperBundle());
    bootstrap.addBundle(new RedirectBundle(new PathRedirect("/", "/app/#/home")));
    bootstrap.addBundle(new AssetsBundle("/app/", "/app", "index.html", "app"));
    bootstrap.addBundle(new AssetsBundle("/assets", "/assets", null, "assets"));
    bootstrap.addBundle(new AssetsBundle("/assets/css", "/assets/css", null, "css"));
    bootstrap.addBundle(new AssetsBundle("/assets/js", "/assets/js", null, "js"));
    bootstrap.addBundle(new AssetsBundle("/assets/lib", "/assets/lib", null, "lib"));
    bootstrap.addBundle(new AssetsBundle("/assets/img", "/assets/img", null, "img"));
    bootstrap.addBundle(new AssetsBundle("/assets/data", "/assets/data", null, "data"));
    bootstrap.addBundle(new ThirdEyeSwaggerBundle());
  }

  @Override
  public void run(ThirdEyeDashboardConfiguration config, Environment env)
      throws Exception {
    LOG.info("isCors value {}", config.isCors());
    if (config.isCors()) {
      FilterRegistration.Dynamic corsFilter = env.servlets().addFilter("CORS", CrossOriginFilter.class);
      corsFilter.setInitParameter(CrossOriginFilter.ALLOWED_METHODS_PARAM, "GET,PUT,POST,DELETE,OPTIONS");
      corsFilter.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, "*");
      corsFilter.setInitParameter(CrossOriginFilter.ALLOWED_HEADERS_PARAM, "Content-Type,Authorization,X-Requested-With,Content-Length,Accept,Origin");
      corsFilter.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");
    }

    super.initDAOs();

    try {
      ThirdEyeCacheRegistry.initializeCaches(config);
    } catch (Exception e) {
      LOG.error("Exception while loading caches", e);
    }

    AnomalyFunctionFactory anomalyFunctionFactory = new AnomalyFunctionFactory(config.getFunctionConfigPath());
    AlertFilterFactory alertFilterFactory = new AlertFilterFactory(config.getAlertFilterConfigPath());

    env.jersey().register(new DetectionConfigurationResource());
    env.jersey().register(new DatasetAutoOnboardResource());
    env.jersey().register(new DashboardResource());
    env.jersey().register(new CacheResource());
    env.jersey().register(new AnomalyResource(alertFilterFactory));
    env.jersey().register(new EntityManagerResource(config));
    env.jersey().register(new MetricConfigResource());
    env.jersey().register(new DatasetConfigResource());
    env.jersey().register(new AdminResource());
    env.jersey().register(new SummaryResource());
    env.jersey().register(new ThirdEyeResource());
    env.jersey().register(new DataResource());
    env.jersey().register(new AnomaliesResource(anomalyFunctionFactory));
    env.jersey().register(new EntityMappingResource());
    env.jersey().register(new OnboardDatasetMetricResource());
    env.jersey().register(new AutoOnboardResource(config));
    env.jersey().register(new ConfigResource(DAO_REGISTRY.getConfigDAO()));
    env.jersey().register(new CustomizedEventResource(DAO_REGISTRY.getEventDAO()));
    env.jersey().register(new AnomalyFlattenResource(DAO_REGISTRY.getMergedAnomalyResultDAO(),
        DAO_REGISTRY.getDatasetConfigDAO(), DAO_REGISTRY.getMetricConfigDAO()));
    env.jersey().register(new UserDashboardResource(
        DAO_REGISTRY.getMergedAnomalyResultDAO(), DAO_REGISTRY.getMetricConfigDAO(), DAO_REGISTRY.getDatasetConfigDAO(),
        DAO_REGISTRY.getDetectionConfigManager(), DAO_REGISTRY.getDetectionAlertConfigManager()));
    env.jersey().register(new ApplicationResource(
        DAO_REGISTRY.getApplicationDAO(), DAO_REGISTRY.getMergedAnomalyResultDAO(),
        DAO_REGISTRY.getDetectionConfigManager(), DAO_REGISTRY.getDetectionAlertConfigManager()));
    env.jersey().register(new DetectionResource());
    env.jersey().register(new DetectionAlertResource(DAO_REGISTRY.getDetectionAlertConfigManager()));
    env.jersey().register(new YamlResource(config.getAlerterConfiguration(), config.getDetectionPreviewConfig(),
        config.getAlertOnboardingPermitPerSecond()));
    env.jersey().register(new SqlDataSourceResource());
    env.jersey().register(new AlertResource());
    env.jersey().register(new HealthCheckResource());

    TimeSeriesLoader timeSeriesLoader = new DefaultTimeSeriesLoader(
        DAO_REGISTRY.getMetricConfigDAO(), DAO_REGISTRY.getDatasetConfigDAO(),
        ThirdEyeCacheRegistry.getInstance().getQueryCache(), ThirdEyeCacheRegistry.getInstance().getTimeSeriesCache());
    AggregationLoader aggregationLoader = new DefaultAggregationLoader(
        DAO_REGISTRY.getMetricConfigDAO(), DAO_REGISTRY.getDatasetConfigDAO(),
        ThirdEyeCacheRegistry.getInstance().getQueryCache(), ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());

    env.jersey().register(new RootCauseSessionResource(
        DAO_REGISTRY.getRootcauseSessionDAO(), new ObjectMapper()));
    env.jersey().register(new RootCauseMetricResource(
        Executors.newCachedThreadPool(), aggregationLoader, timeSeriesLoader,
        DAO_REGISTRY.getMetricConfigDAO(), DAO_REGISTRY.getDatasetConfigDAO()));

    env.getObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    env.getObjectMapper().registerModule(makeMapperModule());

    try {
      // root cause resource
      if (config.getRootCause() != null) {
        env.jersey().register(makeRootCauseResource(config));
      }

      // Load external resources
      if (config.getResourceConfig() != null) {
        List<ResourceConfiguration> resourceConfigurations = config.getResourceConfig();
        for (ResourceConfiguration resourceConfiguration : resourceConfigurations) {
          env.jersey().register(Class.forName(resourceConfiguration.getClassName()));
          LOG.info("Registering resource [{}]", resourceConfiguration.getClassName());
        }
      }
    } catch (Exception e) {
      LOG.error("Error loading the resource", e);
    }

    // Authentication
    if (config.getAuthConfig() != null) {
      final AuthConfiguration authConfig = config.getAuthConfig();

      // default permissive authenticator
      Authenticator<ThirdEyeCredentials, ThirdEyePrincipal> authenticator = new ThirdEyeAuthenticatorDisabled();

      // ldap authenticator
      if (authConfig.isAuthEnabled()) {
        final ThirdEyeLdapAuthenticator
            authenticatorLdap = new ThirdEyeLdapAuthenticator(authConfig.getDomainSuffix(), authConfig.getLdapUrl(), DAORegistry.getInstance().getSessionDAO());
        authenticator = new CachingAuthenticator<>(env.metrics(), authenticatorLdap, CacheBuilder.newBuilder().expireAfterWrite(authConfig.getCacheTTL(), TimeUnit.SECONDS));
      }

      env.jersey().register(new ThirdEyeAuthFilter(authenticator, authConfig.getAllowedPaths(), authConfig.getAdminUsers(), DAORegistry.getInstance().getSessionDAO()));
      env.jersey().register(new AuthResource(authenticator, authConfig.getCookieTTL() * 1000));
      env.jersey().register(new AuthValueFactoryProvider.Binder<>(ThirdEyePrincipal.class));
    }

    if (config.getModelDownloaderConfig() != null) {
      modelDownloaderManager = new ModelDownloaderManager(config.getModelDownloaderConfig());
      modelDownloaderManager.start();
    }

    env.lifecycle().manage(new Managed() {
      @Override
      public void start() throws Exception {
        requestStatisticsLogger = new RequestStatisticsLogger(new TimeGranularity(1, TimeUnit.DAYS));
        requestStatisticsLogger.start();
      }

      @Override
      public void stop() throws Exception {
        if (requestStatisticsLogger != null) {
          requestStatisticsLogger.shutdown();
        }
        if (modelDownloaderManager != null) {
          modelDownloaderManager.shutdown();
        }
      }
    });
  }

  private static RootCauseResource makeRootCauseResource(ThirdEyeDashboardConfiguration config) throws Exception {
    File definitionsFile = getRootCauseDefinitionsFile(config);
    if(!definitionsFile.exists())
      throw new IllegalArgumentException(String.format("Could not find definitions file '%s'", definitionsFile));

    RootCauseConfiguration rcConfig = config.getRootCause();
    return new RootCauseResource(
        makeRootCauseFrameworks(rcConfig, definitionsFile),
        makeRootCauseFormatters(rcConfig));
  }

  private static Map<String, RCAFramework> makeRootCauseFrameworks(RootCauseConfiguration config, File definitionsFile) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(config.getParallelism());
    return RCAFrameworkLoader.getFrameworksFromConfig(definitionsFile, executor);
  }

  private static List<RootCauseEntityFormatter> makeRootCauseFormatters(RootCauseConfiguration config) throws Exception {
    List<RootCauseEntityFormatter> formatters = new ArrayList<>();
    if(config.getFormatters() != null) {
      for(String className : config.getFormatters()) {
        try {
          formatters.add(FormatterLoader.fromClassName(className));
        } catch(ClassNotFoundException e) {
          LOG.warn("Could not find formatter class '{}'. Skipping.", className, e);
        }
      }
    }
    formatters.add(new DefaultEntityFormatter());
    return formatters;
  }

  private static File getRootCauseDefinitionsFile(ThirdEyeDashboardConfiguration config) {
    if(config.getRootCause().getDefinitionsPath() == null)
      throw new IllegalArgumentException("definitionsPath must not be null");
    File rcaConfigFile = new File(config.getRootCause().getDefinitionsPath());
    if(!rcaConfigFile.isAbsolute())
      return new File(config.getRootDir() + File.separator + rcaConfigFile);
    return rcaConfigFile;
  }

  /**
   * The entry point of application.
   *
   * @param args the input arguments
   * @throws Exception the exception
   */
  public static void main(String[] args) throws Exception {
    String thirdEyeConfigDir = "./config";
    if (args.length >= 1) {
      thirdEyeConfigDir = args[0];
    }
    LOG.info("Using config path '{}'", thirdEyeConfigDir);

    System.setProperty("dw.rootDir", thirdEyeConfigDir);
    String dashboardApplicationConfigFile = thirdEyeConfigDir + "/" + "dashboard.yml";
    new ThirdEyeDashboardApplication().run("server", dashboardApplicationConfigFile);
  }

}
