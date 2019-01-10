package com.linkedin.thirdeye.detection.alert;

import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detection.alert.scheme.DetectionAlertScheme;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
  private Map<String, Map<String, Object>> alerters;

  @BeforeMethod
  public void beforeClass() throws Exception {
    Map<String, Object> randomAlerter = new HashMap<>();
    randomAlerter.put("className", "com.linkedin.thirdeye.detection.alert.scheme.RandomAlerter");
    Map<String, Object> anotherRandomAlerter = new HashMap<>();
    anotherRandomAlerter.put("className", "com.linkedin.thirdeye.detection.alert.scheme.AnotherRandomAlerter");

    alerters = new HashMap<>();
    alerters.put("randomScheme", randomAlerter);
    alerters.put("anotherRandomScheme", anotherRandomAlerter);

    this.testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    this.alertConfigDAO = daoRegistry.getDetectionAlertConfigManager();
    this.alertConfigDTO = new DetectionAlertConfigDTO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  private DetectionAlertConfigDTO createAlertConfig(Map<String, Map<String, Object>> schemes, String filter) {
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
    this.alertConfigDAO.save(this.alertConfigDTO);

    return this.alertConfigDTO;
  }

  @Test
  public void testLoadAlertFilter() throws Exception {
    DetectionAlertConfigDTO alertConfig = createAlertConfig(alerters,
        "com.linkedin.thirdeye.detection.alert.filter.ToAllRecipientsDetectionAlertFilter");
    long endTime = 9999l;
    DetectionAlertTaskFactory detectionAlertTaskFactory = new DetectionAlertTaskFactory();
    DetectionAlertFilter detectionAlertFilter = detectionAlertTaskFactory.loadAlertFilter(alertConfig, endTime);

    Assert.assertEquals(detectionAlertFilter.config.getId().longValue(), alertConfig.getId().longValue());
    Assert.assertEquals(detectionAlertFilter.endTime, endTime);
    Assert.assertEquals(detectionAlertFilter.getClass().getSimpleName(), "ToAllRecipientsDetectionAlertFilter");
  }

  @Test
  public void testLoadAlertSchemes() throws Exception {
    DetectionAlertConfigDTO alertConfig = createAlertConfig(alerters,
        "com.linkedin.thirdeye.detection.alert.filter.ToAllRecipientsDetectionAlertFilter");
    DetectionAlertTaskFactory detectionAlertTaskFactory = new DetectionAlertTaskFactory();
    Set<DetectionAlertScheme> detectionAlertSchemes = detectionAlertTaskFactory.loadAlertSchemes(alertConfig,
        new ThirdEyeAnomalyConfiguration(), null);

    Assert.assertEquals(detectionAlertSchemes.size(), 2);
    Iterator<DetectionAlertScheme> alertSchemeIterator = detectionAlertSchemes.iterator();
    Assert.assertTrue(getAlerterSet().contains(alertSchemeIterator.next().getClass().getSimpleName()));
    Assert.assertTrue(getAlerterSet().contains(alertSchemeIterator.next().getClass().getSimpleName()));
  }

  /**
   * Check if an exception is thrown when the detection config id cannot be found
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testDefaultAlertSchemes() throws Exception {
    DetectionAlertTaskFactory detectionAlertTaskFactory = new DetectionAlertTaskFactory();
    detectionAlertTaskFactory.loadAlertSchemes(null, new ThirdEyeAnomalyConfiguration(), null);
  }

  /**
   * Load the default thirdeye email alerter if no scheme is not configured
   */
  @Test
  public void testLoadDefaultAlertSchemes() throws Exception {
    DetectionAlertConfigDTO alertConfig = createAlertConfig(Collections.emptyMap(),
        "com.linkedin.thirdeye.detection.alert.filter.ToAllRecipientsDetectionAlertFilter");
    DetectionAlertTaskFactory detectionAlertTaskFactory = new DetectionAlertTaskFactory();
    Set<DetectionAlertScheme> detectionAlertSchemes = detectionAlertTaskFactory.loadAlertSchemes(alertConfig,
        new ThirdEyeAnomalyConfiguration(), null);

    Assert.assertEquals(detectionAlertSchemes.size(), 1);
    Assert.assertEquals(detectionAlertSchemes.iterator().next().getClass().getSimpleName(), "DetectionEmailAlerter");
  }

  private Set<String> getAlerterSet() {
    return new HashSet<>(Arrays.asList("RandomAlerter", "AnotherRandomAlerter"));
  }
}
