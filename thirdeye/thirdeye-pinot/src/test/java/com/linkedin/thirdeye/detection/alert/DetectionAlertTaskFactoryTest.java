package com.linkedin.thirdeye.detection.alert;

import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detection.alert.scheme.DetectionAlertScheme;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class DetectionAlertTaskFactoryTest {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionAlertTaskFactoryTest.class);

  private DAOTestBase testDAOProvider;
  private DetectionAlertConfigDTO alertConfigDTO;
  private DetectionAlertConfigManager alertConfigDAO;

  @BeforeMethod
  public void beforeClass() throws Exception {
    this.testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    this.alertConfigDAO = daoRegistry.getDetectionAlertConfigManager();
    this.alertConfigDTO = new DetectionAlertConfigDTO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  private long createAlertConfig(String schemes, String filter) {
    Map<String, Object> properties = new HashMap<>();
    properties.put("className", filter);
    properties.put("detectionConfigIds", Collections.singletonList(1000));
    Map<Long, Long> vectorClocks = new HashMap<>();

    this.alertConfigDTO = new DetectionAlertConfigDTO();
    this.alertConfigDTO.setAlertSchemes(schemes);
    this.alertConfigDTO.setProperties(properties);
    this.alertConfigDTO.setFrom("te@linkedin.com");
    this.alertConfigDTO.setName("factory_alert");
    this.alertConfigDTO.setVectorClocks(vectorClocks);

    return this.alertConfigDAO.save(this.alertConfigDTO);
  }

  @Test
  public void testLoadAlertFilter() throws Exception {
    long alertConfigId = createAlertConfig(
        "com.linkedin.thirdeye.detection.alert.scheme.RandomAlerter, com.linkedin.thirdeye.detection.alert.scheme.AnotherRandomAlerter",
        "com.linkedin.thirdeye.detection.alert.filter.ToAllRecipientsDetectionAlertFilter");
    long endTime = 9999l;
    DetectionAlertTaskFactory detectionAlertTaskFactory = new DetectionAlertTaskFactory();
    DetectionAlertFilter detectionAlertFilter = detectionAlertTaskFactory.loadAlertFilter(alertConfigId, endTime);

    Assert.assertEquals(detectionAlertFilter.config.getId().longValue(), alertConfigId);
    Assert.assertEquals(detectionAlertFilter.endTime, endTime);
    Assert.assertEquals(detectionAlertFilter.getClass().getSimpleName(), "ToAllRecipientsDetectionAlertFilter");
  }

  @Test
  public void testLoadAlertSchemes() throws Exception {
    long alertConfigId = createAlertConfig(
        "com.linkedin.thirdeye.detection.alert.scheme.RandomAlerter, com.linkedin.thirdeye.detection.alert.scheme.AnotherRandomAlerter",
        "com.linkedin.thirdeye.detection.alert.filter.ToAllRecipientsDetectionAlertFilter");
    DetectionAlertTaskFactory detectionAlertTaskFactory = new DetectionAlertTaskFactory();
    Set<DetectionAlertScheme> detectionAlertSchemes = detectionAlertTaskFactory.loadAlertSchemes(alertConfigId, null, null);

    Assert.assertEquals(detectionAlertSchemes.size(), 2);
    Iterator<DetectionAlertScheme> alertSchemeIterator = detectionAlertSchemes.iterator();
    Assert.assertTrue(getAlerterSet().contains(alertSchemeIterator.next().getClass().getSimpleName()));
    Assert.assertTrue(getAlerterSet().contains(alertSchemeIterator.next().getClass().getSimpleName()));
  }

  /**
   * Check if an exception is thrown when the detection config id cannot be found
   */
  @Test(expectedExceptions = RuntimeException.class)
  public void testDefaultAlertSchemes() throws Exception {
    DetectionAlertTaskFactory detectionAlertTaskFactory = new DetectionAlertTaskFactory();
    detectionAlertTaskFactory.loadAlertSchemes(9, null, null);
  }

  /**
   * Load the default thirdeye email alerter if no scheme is not configured
   */
  @Test
  public void testLoadDefaultAlertSchemes() throws Exception {
    TaskContext context = new TaskContext();
    context.setThirdEyeAnomalyConfiguration(new ThirdEyeAnomalyConfiguration());
    long alertConfigId = createAlertConfig("",
        "com.linkedin.thirdeye.detection.alert.filter.ToAllRecipientsDetectionAlertFilter");
    DetectionAlertTaskFactory detectionAlertTaskFactory = new DetectionAlertTaskFactory();
    Set<DetectionAlertScheme> detectionAlertSchemes = detectionAlertTaskFactory.loadAlertSchemes(alertConfigId, context, null);

    Assert.assertEquals(detectionAlertSchemes.size(), 1);
    Assert.assertEquals(detectionAlertSchemes.iterator().next().getClass().getSimpleName(), "DetectionEmailAlerter");
  }

  private Set<String> getAlerterSet() {
    return new HashSet<>(Arrays.asList("RandomAlerter", "AnotherRandomAlerter"));
  }
}
