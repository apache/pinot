package com.linkedin.thirdeye.api;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class JoinSpec {

  private List<String> sourceNames;

  private String joinKeyExtractorClass;

  private Map<String,String> joinKeyExtractorConfig;

  private String joinUDFClass;

  private Map<String,String> joinUDFConfig;

  /**
   * Empty constructor needed for serialization/deserialization libraries
   */
  public JoinSpec() {
  }

  /**
   * 
   * @param sourceNames
   * @param joinKeyExtractClass
   * @param joinKeyExtractorConfig
   * @param joinUDFClass
   * @param joinUDFConfig
   */
  public JoinSpec(List<String> sourceNames, String joinKeyExtractClass,
      Map<String,String> joinKeyExtractorConfig, String joinUDFClass,
      Map<String,String> joinUDFConfig) {
    super();
    this.sourceNames = sourceNames;
    this.joinKeyExtractorClass = joinKeyExtractClass;
    this.joinKeyExtractorConfig = joinKeyExtractorConfig;
    this.joinUDFClass = joinUDFClass;
    this.joinUDFConfig = joinUDFConfig;
  }

  public String getJoinKeyExtractorClass() {
    return joinKeyExtractorClass;
  }

  public void setJoinKeyExtractorClass(String joinKeyExtractorClass) {
    this.joinKeyExtractorClass = joinKeyExtractorClass;
  }

  public Map<String,String> getJoinKeyExtractorConfig() {
    return joinKeyExtractorConfig;
  }

  public void setJoinKeyExtractorConfig(Map<String,String> joinKeyExtractorConfig) {
    this.joinKeyExtractorConfig = joinKeyExtractorConfig;
  }

  public String getJoinUDFClass() {
    return joinUDFClass;
  }

  public void setJoinUDFClass(String joinUDFClass) {
    this.joinUDFClass = joinUDFClass;
  }

  public Map<String,String> getJoinUDFConfig() {
    return joinUDFConfig;
  }

  public void setJoinUDFConfig(Map<String,String> joinUDFConfig) {
    this.joinUDFConfig = joinUDFConfig;
  }

  public void setSourceNames(List<String> sourceNames) {
    this.sourceNames = sourceNames;
  }

  public List<String> getSourceNames() {
    return sourceNames;
  }

  public void setSources(List<String> sourceNames) {
    this.sourceNames = sourceNames;
  }
}
