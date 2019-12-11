package org.apache.pinot.thirdeye.detection.alert;

import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.alert.scheme.DetectionAlertScheme;
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

import static org.apache.pinot.thirdeye.notification.commons.SmtpConfiguration.*;


public class DetectionAlertTaskFactoryTest {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionAlertTaskFactoryTest.class);

  private DAOTestBase testDAOProvider;
  private DetectionAlertConfigDTO alertConfigDTO;
  private DetectionAlertConfigManager alertConfigDAO;
  private Map<String, Object> alerters;

  @BeforeMethod
  public void beforeMethod() throws Exception {
    Map<String, Object> randomAlerter = new HashMap<>();
    randomAlerter.put("className", "org.apache.pinot.thirdeye.detection.alert.scheme.RandomAlerter");
    Map<String, Object> anotherRandomAlerter = new HashMap<>();
    anotherRandomAlerter.put("className", "org.apache.pinot.thirdeye.detection.alert.scheme.AnotherRandomAlerter");

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

  private DetectionAlertConfigDTO createAlertConfig(Map<String, Object> schemes, String filter) {
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
        "org.apache.pinot.thirdeye.detection.alert.filter.ToAllRecipientsDetectionAlertFilter");
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
        "org.apache.pinot.thirdeye.detection.alert.filter.ToAllRecipientsDetectionAlertFilter");
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

    ThirdEyeAnomalyConfiguration teConfig = new ThirdEyeAnomalyConfiguration();
    teConfig.setAlerterConfiguration(new HashMap<>());

    detectionAlertTaskFactory.loadAlertSchemes(null, teConfig, null);
  }

  /**
   * Load the default thirdeye email alerter if no scheme is not configured
   */
  @Test
  public void testLoadDefaultAlertSchemes() throws Exception {
    DetectionAlertConfigDTO alertConfig = createAlertConfig(Collections.emptyMap(),
        "org.apache.pinot.thirdeye.detection.alert.filter.ToAllRecipientsDetectionAlertFilter");
    DetectionAlertTaskFactory detectionAlertTaskFactory = new DetectionAlertTaskFactory();

    ThirdEyeAnomalyConfiguration teConfig = new ThirdEyeAnomalyConfiguration();
    teConfig.setAlerterConfiguration(new HashMap<>());

    Set<DetectionAlertScheme> detectionAlertSchemes = detectionAlertTaskFactory.loadAlertSchemes(alertConfig,
        teConfig, null);

    Assert.assertEquals(detectionAlertSchemes.size(), 1);
    Assert.assertEquals(detectionAlertSchemes.iterator().next().getClass().getSimpleName(), "DetectionEmailAlerter");
  }

  private Set<String> getAlerterSet() {
    return new HashSet<>(Arrays.asList("RandomAlerter", "AnotherRandomAlerter"));
  }
}
