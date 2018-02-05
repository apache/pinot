package com.linkedin.thirdeye.alert.content;

import com.linkedin.thirdeye.alert.commons.EmailEntity;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.monitor.MonitorConfiguration;
import com.linkedin.thirdeye.anomaly.task.TaskDriverConfiguration;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyResult;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.datalayer.DaoTestUtils;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.mail.HtmlEmail;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestOnboardingNotificationContentFormatter {
  private static final String TEST = "test";
  private int id = 0;
  private String dashboardHost = "http://localhost:8080/dashboard";
  private DAOTestBase testDAOProvider;
  private AnomalyFunctionManager anomalyFunctionDAO;
  private MergedAnomalyResultManager mergedAnomalyResultDAO;
  private AlertConfigManager alertConfigDAO;
  @BeforeClass
  public void beforeClass(){
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    anomalyFunctionDAO = daoRegistry.getAnomalyFunctionDAO();
    mergedAnomalyResultDAO = daoRegistry.getMergedAnomalyResultDAO();
    alertConfigDAO = daoRegistry.getAlertConfigDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }
  @Test
  public void testGetEmailEntity() throws Exception {
    DateTimeZone dateTimeZone = DateTimeZone.forID("America/Los_Angeles");
    ThirdEyeAnomalyConfiguration thirdeyeAnomalyConfig = new ThirdEyeAnomalyConfiguration();
    thirdeyeAnomalyConfig.setId(id);
    thirdeyeAnomalyConfig.setDashboardHost(dashboardHost);
    MonitorConfiguration monitorConfiguration = new MonitorConfiguration();
    monitorConfiguration.setMonitorFrequency(new TimeGranularity(3, TimeUnit.SECONDS));
    thirdeyeAnomalyConfig.setMonitorConfiguration(monitorConfiguration);
    TaskDriverConfiguration taskDriverConfiguration = new TaskDriverConfiguration();
    taskDriverConfiguration.setNoTaskDelayInMillis(1000);
    taskDriverConfiguration.setRandomDelayCapInMillis(200);
    taskDriverConfiguration.setTaskFailureDelayInMillis(500);
    taskDriverConfiguration.setMaxParallelTasks(2);
    thirdeyeAnomalyConfig.setTaskDriverConfiguration(taskDriverConfiguration);
    thirdeyeAnomalyConfig.setRootDir(System.getProperty("dw.rootDir", "NOT_SET(dw.rootDir)"));

    List<AnomalyResult> anomalies = new ArrayList<>();
    AnomalyFunctionDTO anomalyFunction = DaoTestUtils.getTestFunctionSpec(TEST, TEST);
    anomalyFunctionDAO.save(anomalyFunction);
    MergedAnomalyResultDTO anomaly = DaoTestUtils.getTestMergedAnomalyResult(
        new DateTime(2017, 11, 6, 10, 0, dateTimeZone).getMillis(),
        new DateTime(2017, 11, 6, 13, 0, dateTimeZone).getMillis(),
        TEST, TEST, 0.1, 1l, new DateTime(2017, 11, 6, 10, 0, dateTimeZone).getMillis());
    anomaly.setFunction(anomalyFunction);
    anomaly.setAvgCurrentVal(1.1);
    anomaly.setAvgBaselineVal(1.0);
    mergedAnomalyResultDAO.save(anomaly);
    anomalies.add(anomaly);
    anomaly = DaoTestUtils.getTestMergedAnomalyResult(
        new DateTime(2017, 11, 7, 10, 0, dateTimeZone).getMillis(),
        new DateTime(2017, 11, 7, 17, 0, dateTimeZone).getMillis(),
        TEST, TEST, 0.1, 1l, new DateTime(2017, 11, 6, 10, 0, dateTimeZone).getMillis());
    anomaly.setFunction(anomalyFunction);
    anomaly.setAvgCurrentVal(0.9);
    anomaly.setAvgBaselineVal(1.0);
    mergedAnomalyResultDAO.save(anomaly);
    anomalies.add(anomaly);

    AlertConfigDTO alertConfigDTO = DaoTestUtils.getTestAlertConfiguration("Test Config");
    alertConfigDAO.save(alertConfigDTO);

    EmailContentFormatterContext context = new EmailContentFormatterContext();
    context.setAnomalyFunctionSpec(anomalyFunction);
    context.setAlertConfig(alertConfigDTO);
    EmailContentFormatter contentFormatter = new OnboardingNotificationEmailContentFormatter();
    contentFormatter.init(new Properties(), EmailContentFormatterConfiguration.fromThirdEyeAnomalyConfiguration(thirdeyeAnomalyConfig));
    EmailEntity emailEntity = contentFormatter.getEmailEntity(alertConfigDTO, "a@b.com", TEST,
        null, "", anomalies, context);

    String htmlPath = ClassLoader.getSystemResource("test-onboard-notification-email-content-formatter.html").getPath();
    BufferedReader br = new BufferedReader(new FileReader(htmlPath));
    StringBuilder htmlContent = new StringBuilder();
    for(String line = br.readLine(); line != null; line = br.readLine()) {
      htmlContent.append(line + "\n");
    }
    br.close();

    HtmlEmail email = emailEntity.getContent();
    Field field = email.getClass().getDeclaredField("html");
    field.setAccessible(true);
    String emailHtml = field.get(email).toString();
    Assert.assertEquals(emailHtml.replaceAll("\\s", ""),
        htmlContent.toString().replaceAll("\\s", ""));
  }

}
