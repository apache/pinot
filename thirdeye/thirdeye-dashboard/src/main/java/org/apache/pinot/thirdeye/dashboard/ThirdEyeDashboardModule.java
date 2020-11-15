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
 *
 */

package org.apache.pinot.thirdeye.dashboard;

import com.codahale.metrics.MetricRegistry;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.CachingAuthenticator;
import io.dropwizard.setup.Environment;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.auth.ThirdEyeAuthFilter;
import org.apache.pinot.thirdeye.auth.ThirdEyeAuthenticatorDisabled;
import org.apache.pinot.thirdeye.auth.ThirdEyeCredentials;
import org.apache.pinot.thirdeye.auth.ThirdEyeLdapAuthenticator;
import org.apache.pinot.thirdeye.auth.ThirdEyePrincipal;
import org.apache.pinot.thirdeye.common.ThirdEyeConfiguration;
import org.apache.pinot.thirdeye.dashboard.configs.AuthConfiguration;
import org.apache.pinot.thirdeye.dashboard.resources.v2.AuthResource;
import org.apache.pinot.thirdeye.dashboard.resources.v2.RootCauseMetricResource;
import org.apache.pinot.thirdeye.dashboard.resources.v2.RootCauseResource;
import org.apache.pinot.thirdeye.datalayer.bao.ApplicationManager;
import org.apache.pinot.thirdeye.datalayer.bao.ConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.EvaluationManager;
import org.apache.pinot.thirdeye.datalayer.bao.EventManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.RootcauseSessionManager;
import org.apache.pinot.thirdeye.datalayer.bao.SessionManager;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.cache.QueryCache;
import org.apache.pinot.thirdeye.datasource.loader.AggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultAggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import org.apache.pinot.thirdeye.datasource.loader.TimeSeriesLoader;
import org.apache.pinot.thirdeye.detection.cache.TimeSeriesCache;
import org.apache.pinot.thirdeye.detection.yaml.YamlResource;
import org.apache.pinot.thirdeye.detector.email.filter.AlertFilterFactory;
import org.apache.pinot.thirdeye.detector.function.AnomalyFunctionFactory;
import org.apache.pinot.thirdeye.model.download.ModelDownloaderManager;

/**
 * This is the main Guice module for ThirdEye Dashboard Server.
 * All resources, dependencies are fed from bindings in this class.
 */
public class ThirdEyeDashboardModule extends AbstractModule {

  private final ThirdEyeDashboardConfiguration config;
  private final Environment environment;

  // TODO spyne After refactoring bindinds, remove this dependency
  private final DAORegistry DAO_REGISTRY;

  public ThirdEyeDashboardModule(final ThirdEyeDashboardConfiguration config,
      final Environment environment,
      final DAORegistry DAO_REGISTRY) {
    this.config = config;
    this.environment = environment;
    this.DAO_REGISTRY = DAO_REGISTRY;
  }

  @Override
  protected void configure() {
    /*
     * Since the superclass is not abstract, this requires an explicit binding to the subclass
     *      config class else Guice will create a new instance of config
     */
    bind(ThirdEyeConfiguration.class).to(ThirdEyeDashboardConfiguration.class);
    bind(ThirdEyeDashboardConfiguration.class).toInstance(config);
    bind(TimeSeriesLoader.class).to(DefaultTimeSeriesLoader.class);
    bind(MetricRegistry.class).toInstance(environment.metrics());

    /*
     * TODO spyne Refactor DAO_REGISTRY bindings
     *
     * These bindings added here are a temporary hack. The goal is to leverage the DataSourceModule
     * module directly to inject these classes. That way, all dependencies can be injected by Guice.
     * This will be done in subsequent phases of refactoring.
     */
    bind(ConfigManager.class).toInstance(DAO_REGISTRY.getConfigDAO());
    bind(EventManager.class).toInstance(DAO_REGISTRY.getEventDAO());
    bind(EvaluationManager.class).toInstance(DAO_REGISTRY.getEvaluationManager());
    bind(MergedAnomalyResultManager.class).toInstance(DAO_REGISTRY.getMergedAnomalyResultDAO());
    bind(DatasetConfigManager.class).toInstance(DAO_REGISTRY.getDatasetConfigDAO());
    bind(MetricConfigManager.class).toInstance(DAO_REGISTRY.getMetricConfigDAO());
    bind(DetectionConfigManager.class).toInstance(DAO_REGISTRY.getDetectionConfigManager());
    bind(RootcauseSessionManager.class).toInstance(DAO_REGISTRY.getRootcauseSessionDAO());
    bind(SessionManager.class).toInstance(DAO_REGISTRY.getSessionDAO());
    bind(DetectionAlertConfigManager.class)
        .toInstance(DAO_REGISTRY.getDetectionAlertConfigManager());
    bind(ApplicationManager.class).toInstance(DAO_REGISTRY.getApplicationDAO());
    bind(QueryCache.class).toInstance(ThirdEyeCacheRegistry.getInstance().getQueryCache());
    bind(TimeSeriesCache.class)
        .toInstance(ThirdEyeCacheRegistry.getInstance().getTimeSeriesCache());
    bind(RootCauseResource.class)
        .toProvider(new RootCauseResourceProvider(config))
        .in(Scopes.SINGLETON);
  }

  @Singleton
  @Provides
  public AnomalyFunctionFactory getAnomalyFunctionFactory() {
    return new AnomalyFunctionFactory(config.getFunctionConfigPath());
  }

  @Singleton
  @Provides
  public AlertFilterFactory getAlertFilterFactory() {
    return new AlertFilterFactory(config.getAlertFilterConfigPath());
  }

  @Singleton
  @Provides
  public YamlResource getYamlResource() {
    return new YamlResource(config.getAlerterConfiguration(),
        config.getDetectionPreviewConfig(),
        config.getAlertOnboardingPermitPerSecond());
  }

  @Singleton
  @Provides
  public ModelDownloaderManager getModelDownloaderManager() {
    return new ModelDownloaderManager(config.getModelDownloaderConfig());
  }

  @Singleton
  @Provides
  public AggregationLoader getAggregationLoader(final QueryCache queryCache) {
    return new DefaultAggregationLoader(DAO_REGISTRY.getMetricConfigDAO(),
        DAO_REGISTRY.getDatasetConfigDAO(),
        queryCache,
        ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());
  }

  @Singleton
  @Provides
  public RootCauseMetricResource getRootCauseMetricResource(
      final AggregationLoader aggregationLoader,
      final TimeSeriesLoader timeSeriesLoader,
      final MetricConfigManager metricConfigManager,
      final DatasetConfigManager datasetConfigManager,
      final EventManager eventManager,
      final MergedAnomalyResultManager mergedAnomalyResultManager,
      final EvaluationManager evaluationManager) {
    return new RootCauseMetricResource(Executors.newCachedThreadPool(),
        aggregationLoader,
        timeSeriesLoader,
        metricConfigManager,
        datasetConfigManager,
        eventManager,
        mergedAnomalyResultManager,
        evaluationManager);
  }

  @Singleton
  @Provides
  public Authenticator<ThirdEyeCredentials, ThirdEyePrincipal> getAuthenticator(
      final MetricRegistry metricRegistry,
      final SessionManager sessionManager) {
    final AuthConfiguration authConfig = config.getAuthConfig();

    // default permissive authenticator
    Authenticator<ThirdEyeCredentials, ThirdEyePrincipal> authenticator = new ThirdEyeAuthenticatorDisabled();

    // ldap authenticator
    if (authConfig.isAuthEnabled()) {
      final ThirdEyeLdapAuthenticator
          authenticatorLdap = new ThirdEyeLdapAuthenticator(
          authConfig.getDomainSuffix(),
          authConfig.getLdapUrl(),
          sessionManager);
      authenticator = new CachingAuthenticator<>(
          metricRegistry,
          authenticatorLdap,
          Caffeine.newBuilder().expireAfterWrite(authConfig.getCacheTTL(), TimeUnit.SECONDS));
    }
    return authenticator;
  }


  @Singleton
  @Provides
  public ThirdEyeAuthFilter getThirdEyeAuthFilter(
      final Authenticator<ThirdEyeCredentials, ThirdEyePrincipal> authenticator) {
    final AuthConfiguration authConfig = config.getAuthConfig();
    return new ThirdEyeAuthFilter(authenticator,
        authConfig.getAllowedPaths(),
        authConfig.getAdminUsers(),
        DAORegistry.getInstance().getSessionDAO());
  }

  @Singleton
  @Provides
  public AuthResource getAuthResource(
      final Authenticator<ThirdEyeCredentials, ThirdEyePrincipal> authenticator) {
    final AuthConfiguration authConfig = config.getAuthConfig();
    return new AuthResource(authenticator, authConfig.getCookieTTL() * 1000);
  }
}
