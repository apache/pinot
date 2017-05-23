package com.linkedin.thirdeye.completeness.checker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.datalayer.pojo.DatasetConfigBean;

/**
 * This class serves as the input for the call to the endpoint which determines percent completeness
 */
@JsonIgnoreProperties(ignoreUnknown=true)
public class PercentCompletenessFunctionInput {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Logger LOG = LoggerFactory.getLogger(PercentCompletenessFunctionInput.class);

  private List<Long> baselineCounts = new ArrayList<>();
  private Long currentCount = 0L;
  private String algorithmClass = DatasetConfigBean.DEFAULT_COMPLETENESS_ALGORITHM;

  public List<Long> getBaselineCounts() {
    return baselineCounts;
  }
  public void setBaselineCounts(List<Long> baselineCounts) {
    this.baselineCounts = baselineCounts;
  }
  public Long getCurrentCount() {
    return currentCount;
  }
  public void setCurrentCount(Long currentCount) {
    this.currentCount = currentCount;
  }
  public String getAlgorithmClass() {
    return algorithmClass;
  }
  public void setAlgorithmClass(String algorithmClass) {
    this.algorithmClass = algorithmClass;
  }

  public static String toJson(PercentCompletenessFunctionInput input) {
    String jsonString = null;
    try {
      jsonString = OBJECT_MAPPER.writeValueAsString(input);
    } catch (JsonProcessingException e) {
      LOG.error("Exception in converting object {} to json string", input, e);
    }
    return jsonString;
  }

  public static PercentCompletenessFunctionInput fromJson(String jsonString) {
    PercentCompletenessFunctionInput input = null;
    try {
      input = OBJECT_MAPPER.readValue(jsonString, PercentCompletenessFunctionInput.class);
    } catch (IOException e) {
      LOG.info("Exception in converting json string {} to PercentCompletenessFunctionInput", jsonString, e);
    }
    return input;
  }



}
