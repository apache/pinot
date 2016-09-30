package com.linkedin.thirdeye.datalayer.dto;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.linkedin.thirdeye.datalayer.pojo.WebappConfigBean;

/**
 * Entity class for webapp configs. name, collection, type conbination should be unique
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WebappConfigDTO extends WebappConfigBean {
}
