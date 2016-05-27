package com.linkedin.thirdeye.client;

import org.codehaus.jackson.map.ObjectMapper;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class MetricFunction implements Comparable<MetricFunction> {

  public static final String SUM = "sum"; // TODO should this be an enum?

  private String functionName;
  private String metricName;
  private String metricFunctionString;
  public MetricFunction(){
    
  }

  public MetricFunction(@JsonProperty("functionName") String functionName,
      @JsonProperty("metricName") String metricName) {
    this.functionName = functionName;
    // this.functionName = function.toString();
    this.metricName = metricName;
    // TODO this is hardcoded for pinot's return column name, but there's no binding contract that
    // clients need to return response objects with these keys.
    this.metricFunctionString = format(functionName, metricName);
  }

  private String format(String functionName, String metricName) {
    return String.format("%s_%s", functionName, metricName);
  }

  public static MetricFunction from(@JsonProperty("functionName") String functionName,
      @JsonProperty("metricName") String metricName) {
    return new MetricFunction(functionName, metricName);
  }

  @Override
  public String toString() {
    if(metricFunctionString == null){
      metricFunctionString = format(functionName, metricName);
    }
    return metricFunctionString;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(functionName, metricName);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof MetricFunction)) {
      return false;
    }
    MetricFunction that = (MetricFunction) obj;
    return Objects.equal(this.functionName, that.functionName)
        && Objects.equal(this.metricName, that.metricName);
  }

  @Override
  public int compareTo(MetricFunction o) {
    return this.toString().compareTo(o.toString());
  }

  public String getFunctionName() {
    return functionName;
  }

  public String getMetricName() {
    return metricName;
  }
  
  public void setMetricName(String metricName) {
    this.metricName = metricName;
  }
  
  public void setFunctionName(String functionName) {
    this.functionName = functionName;
  }

  public static void main(String[] args) throws Exception{
    ObjectMapper objectMapper = new ObjectMapper();
    
    MetricFunction func = new MetricFunction("SUM", "__COUNT");
    String value = objectMapper.writeValueAsString(func);
    System.out.println(value);
    MetricFunction readValue = objectMapper.readValue(value.getBytes(), MetricFunction.class);
    System.out.println(readValue);
    
  }
}
