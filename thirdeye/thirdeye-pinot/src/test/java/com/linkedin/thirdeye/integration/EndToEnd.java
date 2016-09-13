package com.linkedin.thirdeye.integration;

import java.io.File;
import java.io.InputStream;
import java.util.List;

import org.mockito.Mockito;
import org.quartz.SchedulerException;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.thirdeye.anomaly.SmtpConfiguration;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.alert.AlertJobScheduler;
import com.linkedin.thirdeye.anomaly.detection.DetectionJobScheduler;
import com.linkedin.thirdeye.anomaly.monitor.MonitorJobScheduler;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.anomaly.task.TaskDriver;
import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.cache.CollectionSchemaCacheLoader;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClient;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientConfig;
import com.linkedin.thirdeye.dashboard.configs.CollectionConfig;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;


public class EndToEnd extends AbstractRunnerDbTestBase {

  private DetectionJobScheduler detectionJobScheduler = null;
  private TaskDriver taskDriver = null;
  private MonitorJobScheduler monitorJobScheduler = null;
  private AlertJobScheduler alertJobScheduler = null;
  private AnomalyFunctionFactory anomalyFunctionFactory = null;
  private ThirdEyeCacheRegistry cacheRegistry = ThirdEyeCacheRegistry.getInstance();
  private Schema pinotSchema;
  private ThirdEyeAnomalyConfiguration thirdeyeAnomalyConfig;

  private int id = 0;
  private String dashboardHost = "http://localhost:8080/dashboard";
  //private String smtpHost = "email.corp.linkedin.com";
  //private int smtpPort = 25;
  //private String failureEmail = "npawar@linkedin.com";
  //private String whitelist = "thirdeyeKbmi";
  //private String zookeeperUrl = "zk-lva1-pinot.corp.linkedin.com:12913/pinot-cluster";
  //private String clusterName = "mpSprintDemoCluster";
  //private String controllerHost = "lva1-pinot-controller-vip-1.corp.linkedin.com";
  //private int controllerPort = 11984;
  private String functionPropertiesFile = "/sample-config-dir/detector-config/anomaly-functions/functions.properties";
  private String thirdeyeCollectionSchema = "/sample-config-dir/webapp-config/CollectionSchema/test-collection.json";
  private String samplePinotSchema = "/sample-pinot-schema.json";
  private String metric = "count";
  private String collection = "test-collection";

  private void setup() throws Exception {
    // Query cache
    QueryCache mockQueryCache = Mockito.mock(QueryCache.class);
    Mockito.when(mockQueryCache.getClient()).thenReturn(null);
    Mockito.when(mockQueryCache.getQueryResult(null)).thenReturn(null);
    Mockito.when(mockQueryCache.getQueryResultAsync(null)).thenReturn(null);
    Mockito.when(mockQueryCache.getQueryResultsAsync(null)).thenReturn(null);
    cacheRegistry.registerQueryCache(mockQueryCache);

    // Collection Schema Cache
    CollectionSchema testCollectionSchema =
        CollectionSchema.decode(EndToEnd.class.getResourceAsStream(thirdeyeCollectionSchema));
    LoadingCache<String, CollectionSchema> mockCollectionSchemaCache = Mockito.mock(LoadingCache.class);
    Mockito.when(mockCollectionSchemaCache.get(collection)).thenReturn(testCollectionSchema);
    cacheRegistry.registerCollectionSchemaCache(mockCollectionSchemaCache);

    // CollectionConfig Cache
    CollectionConfig testCollectionConfig = new CollectionConfig();
    testCollectionConfig.setCollectionName(collection);
    LoadingCache<String, CollectionConfig> mockCollectionConfigCache = Mockito.mock(LoadingCache.class);
    Mockito.when(mockCollectionConfigCache.get(collection)).thenReturn(testCollectionConfig);
    cacheRegistry.registerCollectionConfigCache(mockCollectionConfigCache);

    /*cacheRegistry.registerCollectionAliasCache(collectionAliasCache);
    cacheRegistry.registerCollectionConfigCache(collectionConfigCache);
    cacheRegistry.registerCollectionMaxDataTimeCache(collectionMaxDataTimeCache);
    cacheRegistry.registerCollectionsCache(collectionsCache);
    cacheRegistry.registerCollectionSchemaCache(collectionSchemaCache);
    cacheRegistry.registerDashboardsCache(dashboardsCache);
    cacheRegistry.registerDimensionFiltersCache(dimensionFiltersCache);
    cacheRegistry.registerResultSetGroupCache(resultSetGroupCache);
    cacheRegistry.registerSchemaCache(schemaCache);*/

    thirdeyeAnomalyConfig = new ThirdEyeAnomalyConfiguration();
    thirdeyeAnomalyConfig.setId(id);
    thirdeyeAnomalyConfig.setDashboardHost(dashboardHost);
    //SmtpConfiguration smtpConfiguration = new SmtpConfiguration();
    //smtpConfiguration.setSmtpHost(smtpHost);
    //smtpConfiguration.setSmtpPort(smtpPort);
    //config.setSmtpConfiguration(smtpConfiguration);
    //config.setFailureFromAddress(failureEmail);
    //config.setFailureToAddress(failureEmail);
    //config.setWhitelistCollections(whitelist);

    /*PinotThirdEyeClientConfig pinotThirdeyeClientConfig = new PinotThirdEyeClientConfig();
    pinotThirdeyeClientConfig.setZookeeperUrl(zookeeperUrl);
    pinotThirdeyeClientConfig.setClusterName(clusterName);
    pinotThirdeyeClientConfig.setControllerHost(controllerHost);
    pinotThirdeyeClientConfig.setControllerPort(controllerPort);*/

    File pinotSchemaFile = new File(EndToEnd.class.getResource(samplePinotSchema).toURI());
    pinotSchema = Schema.fromFile(pinotSchemaFile);

    // create anomaly function
    anomalyFunctionDAO.save(getTestFunctionSpec(metric, collection));

    // create email configuration
    emailConfigurationDAO.save(getTestEmailConfiguration(metric, collection));
  }

  @Test
  public void testEndToEnd() throws Exception {

    setup();

    detectionJobScheduler = new DetectionJobScheduler(anomalyJobDAO, anomalyTaskDAO, anomalyFunctionDAO);
    detectionJobScheduler.start();
    Thread.sleep(10000);
    Assert.assertEquals(1, anomalyJobDAO.findAll().size());
    Assert.assertEquals(1, anomalyTaskDAO.findAll().size());
    Thread.sleep(10000);
    Assert.assertEquals(2, anomalyJobDAO.findAll().size());
    Assert.assertEquals(2, anomalyTaskDAO.findAll().size());


    alertJobScheduler = new AlertJobScheduler(anomalyJobDAO, anomalyTaskDAO, emailConfigurationDAO);
    alertJobScheduler.start();
    Thread.sleep(10000);
    Assert.assertEquals(4, anomalyJobDAO.findAll().size());
    Assert.assertEquals(4, anomalyTaskDAO.findAll().size());
    Thread.sleep(10000);
    Assert.assertEquals(6, anomalyJobDAO.findAll().size());
    Assert.assertEquals(6, anomalyTaskDAO.findAll().size());

    //monitorJobScheduler = new MonitorJobScheduler(anomalyJobDAO, anomalyTaskDAO, config.getMonitorConfiguration());
    //monitorJobScheduler.start();

    // start workers, schedulers, monitor
    InputStream factoryStream = EndToEnd.class.getResourceAsStream(functionPropertiesFile);
    anomalyFunctionFactory = new AnomalyFunctionFactory(factoryStream);

    taskDriver = new TaskDriver(thirdeyeAnomalyConfig, anomalyJobDAO, anomalyTaskDAO, anomalyResultDAO, mergedResultDAO,
        anomalyFunctionFactory);
    taskDriver.start();

    /*List<TaskDTO> anomalyTasks = anomalyTaskDAO.findAll();
    int waitCount = 0;
    for (TaskDTO task : anomalyTasks) {
      if (task.getStatus().equals(TaskStatus.WAITING)) {
        waitCount++;
      }
    }
    Assert.assertTrue(waitCount > 0);
    Thread.sleep(30000);
    anomalyTasks = anomalyTaskDAO.findAll();
    int completeCount = 0;
    for (TaskDTO task : anomalyTasks) {
      if (task.getStatus().equals(TaskStatus.COMPLETED)) {
        completeCount++;
      }
    }
    Assert.assertTrue(completeCount > 0);*/

    int ch = '0';
    while(ch != 'q') {
      System.out.println("press q to quit");
      ch = System.in.read();
      if(ch == 'q') {
        if (detectionJobScheduler != null) detectionJobScheduler.stop();
        if (alertJobScheduler != null) alertJobScheduler.stop();
        if (taskDriver != null) taskDriver.stop();
        break;
      }
    }
  }

}
