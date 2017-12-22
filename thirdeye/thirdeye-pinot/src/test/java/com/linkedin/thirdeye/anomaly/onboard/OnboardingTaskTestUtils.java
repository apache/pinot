package com.linkedin.thirdeye.anomaly.onboard;

import com.linkedin.thirdeye.anomaly.onboard.tasks.DefaultDetectionOnboardJob;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;


public class OnboardingTaskTestUtils {
  public static String TEST_COLLECTION = "test_dataset";
  public static String TEST_METRIC = "test_metric";

  /**
   * Generate a default job properties for all onboarding tests
   * @return a job properties
   */
  public static Map<String, String> getJobProperties(){
    Map<String, String> properties = new HashMap<>();
    properties.put(
        DefaultDetectionOnboardJob.FUNCTION_FACTORY_CONFIG_PATH, ClassLoader.getSystemResource("sample-functions.properties").getPath());
    properties.put(DefaultDetectionOnboardJob.ALERT_FILTER_FACTORY_CONFIG_PATH, ClassLoader.getSystemResource("sample-alertfilter.properties").getPath());
    properties.put(DefaultDetectionOnboardJob.ALERT_FILTER_AUTOTUNE_FACTORY_CONFIG_PATH, ClassLoader.getSystemResource("sample-alertfilter-autotune.properties").getPath());
    properties.put(DefaultDetectionOnboardJob.FUNCTION_NAME, "Normal Function");
    properties.put(DefaultDetectionOnboardJob.COLLECTION_NAME, TEST_COLLECTION);
    properties.put(DefaultDetectionOnboardJob.METRIC_NAME, TEST_METRIC);
    properties.put(DefaultDetectionOnboardJob.WINDOW_SIZE, "1");
    properties.put(DefaultDetectionOnboardJob.WINDOW_UNIT, "DAYS");
    properties.put(DefaultDetectionOnboardJob.CRON_EXPRESSION, "0 0 0 1/1 * ? *");
    properties.put(DefaultDetectionOnboardJob.FUNCTION_PROPERTIES, "metricTimezone=America/Los_Angeles;");
    properties.put(DefaultDetectionOnboardJob.ALERT_NAME, "Normal Alert");
    properties.put(DefaultDetectionOnboardJob.ALERT_TO, "test@test.com");
    properties.put(DefaultDetectionOnboardJob.SMTP_HOST, "test.com");
    properties.put(DefaultDetectionOnboardJob.SMTP_PORT, "25");
    properties.put(DefaultDetectionOnboardJob.DEFAULT_ALERT_RECEIVER_ADDRESS, "test@test.com");
    properties.put(DefaultDetectionOnboardJob.DEFAULT_ALERT_SENDER_ADDRESS, "test@test.com");
    properties.put(DefaultDetectionOnboardJob.THIRDEYE_DASHBOARD_HOST, "test.com");
    properties.put(DefaultDetectionOnboardJob.PHANTON_JS_PATH, "/");
    properties.put(DefaultDetectionOnboardJob.ROOT_DIR, "/");

    return properties;
  }

  /**
   * Return a default task configuration for all onboarding tasks
   * @return a task context
   */
  public static DetectionOnboardTaskContext getDetectionTaskContext() {
    Configuration configuration = new MapConfiguration(getJobProperties());
    DetectionOnboardTaskContext detectionOnboardTaskContext = new DetectionOnboardTaskContext();
    detectionOnboardTaskContext.setConfiguration(configuration);
    return detectionOnboardTaskContext;
  }
}
