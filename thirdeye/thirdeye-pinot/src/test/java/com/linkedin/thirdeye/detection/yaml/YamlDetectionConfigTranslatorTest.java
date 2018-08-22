package com.linkedin.thirdeye.detection.yaml;

import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class YamlDetectionConfigTranslatorTest {

  @Test
  public void testConvertYamlToDetectionConfig() {
    YamlDetectionConfigTranslator translator = new YamlDetectionConfigTranslator();
    String yaml = "flowType: advanced\n" + "name: NameOfThisDetection\n" + "metricName: page_view\n"
        + "dataset: business_intraday_metrics_dim_rt_hourly_additive\n" + "cron: 15 10/15 * * * ? *\n"
        + "dimensions: \n" + "    - ip_country_code\n" + "    - page_key\n" + "filters:\n" + "    continent: \n"
        + "       - Europe\n" + "       - Asia\n" + "    os_name: \n" + "       - android\n" + "    browserName: \n"
        + "       - chrome\n" + "\n" + "algorithmDetection: \n" + "\n" + "      key: value\n" + "\n"
        + "ruleDetection:\n" + "      type: BASELINE\n" + "      properties:\n" + "            change: 10%\n"
        + "ruleFilter:\n" + "      type: SITEWIDE_IMPACT\n" + "      properties:\n" + "             min: 5%\n" + "\n";
    translator.convertYamlToDetectionConfig(yaml);
  }
}