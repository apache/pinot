package com.linkedin.thirdeye.datalayer.dto;

import com.linkedin.thirdeye.datalayer.pojo.AutotuneConfigBean;
import com.linkedin.thirdeye.detector.email.filter.AlertFilter;
import com.linkedin.thirdeye.detector.email.filter.BaseAlertFilter;
import com.linkedin.thirdeye.detector.email.filter.DummyAlertFilter;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class AutotuneConfigDTO extends AutotuneConfigBean {
  private static final Logger LOGGER = LoggerFactory.getLogger(AutotuneConfigDTO.class);
  private AlertFilter alertFilter = new DummyAlertFilter();
  private Properties properties;

  public AutotuneConfigDTO() {

  }

  public AutotuneConfigDTO(BaseAlertFilter alertFilter){
    setAlertFilter(alertFilter);
    this.properties = alertFilter.toProperties();
  }

  public AutotuneConfigDTO(Properties properties) {
    setProperties(properties);
  }


  public void setAlertFilter(AlertFilter alertFilter) {
    this.alertFilter = alertFilter;
  }

  public AlertFilter getAlertFilter() {
    return this.alertFilter;
  }

  public void setProperties(Properties properties) {
    this.properties = properties;
  }

  public Properties getProperties() {
    return this.properties;
  }

}
