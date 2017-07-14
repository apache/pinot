package com.linkedin.thirdeye.datalayer.dto;

import com.linkedin.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AnomalyFunctionDTO extends AnomalyFunctionBean {
  private static final Logger LOGGER = LoggerFactory.getLogger(AnomalyFunctionDTO.class);

  public String getTopicMetric() {
    return getMetric();
  }

  public void setTopicMetric(String topicMetric) {
    setMetric(topicMetric);
  }

  // TODO: Remove this method and update the value in DB instead
  /**
   * Returns the list of metrics to be retrieved for anomaly detection. If the information is not
   * provided in the DB, then it returns a list that contains only topic metric.
   *
   * @return a list of metrics to be retrieved for anomaly detection.
   */
  @Override
  public List<String> getMetrics() {
    if (CollectionUtils.isEmpty((super.getMetrics()))) {
      return Arrays.asList(getMetric());
    } else {
      return super.getMetrics();
    }
  }

  /**
   * Return if this function should get total metric for anomaly calculation
   * @return
   * true if this function should get total metric
   */
  public boolean isToCalculateGlobalMetric() {
    return StringUtils.isNotEmpty(this.getGlobalMetric());
  }

  /**
   * Parses the properties of String and returns the corresponding Properties object.
   *
   * @return a Properties object corresponds to the properties String of this anomaly function.
   */
  public Properties toProperties() {
    Properties props = null;
    try {
      props = toProperties(getProperties());
    } catch (IOException e) {
      LOGGER.warn("Failed to parse property string ({}) for anomaly function: {}", getProperties(), getId());

    }
    return props;
  }

  /**
   * Parses the properties of String and returns the corresponding Properties object.
   *
   * @return a Properties object corresponds to the properties String of this anomaly function.
   */
  public static Properties toProperties(String properties) throws IOException {
    Properties props = new Properties();

    if (properties != null) {
      String[] tokens = properties.split(";");
      for (String token : tokens) {
        try {
          props.load(new ByteArrayInputStream(token.getBytes()));
        } catch (IOException e) {
        }
      }
    }
    return props;
  }

  /**
   * Convert Properties to String following the format in TE
   */
  public String propertiesToString(Properties props){
    StringBuilder stringBuilder = new StringBuilder();
    for(Map.Entry entry : props.entrySet()){
      stringBuilder.append(entry.getKey() + "=" + entry.getValue() + ";");
    }
    stringBuilder.deleteCharAt(stringBuilder.length() - 1);
    return stringBuilder.toString();
  }

  /**
   * Update values of entries in Properties
   * @param config
   * the configuration need to be updated in the function properties
   */
  public void updateProperties(Map<String, String> config) {
    Properties properties = toProperties();
    for (Map.Entry<String, String> entry : config.entrySet()) {
      properties.setProperty(entry.getKey(), entry.getValue());
    }
    setProperties(propertiesToString(properties));
  }
}
