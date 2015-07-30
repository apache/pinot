package com.linkedin.thirdeye.reporting.api;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class ReportConfig {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

  private String name;
  private String collection;
  private List<TableSpec> tables;
  private AliasSpec aliases;
  private DateTime endTime;
  private DateTime startTime;
  private Map<String, ScheduleSpec> schedules;

  public ReportConfig() {

  }

  public DateTime getEndTime() {
    return endTime;
  }

  public void setEndTime(DateTime endTime) {
    this.endTime = endTime;
  }

  public DateTime getStartTime() {
    return startTime;
  }

  public void setStartTime(DateTime startTime) {
    this.startTime = startTime;
  }


  public AliasSpec getAliases() {
    return aliases;
  }

  public String getName() {
    return name;
  }

  public String getCollection() {
    return collection;
  }

  public Map<String, ScheduleSpec> getSchedules() {
    return schedules;
  }


  public List<TableSpec> getTables() {
    return tables;
  }


  public static ReportConfig decode(InputStream inputStream) throws IOException
  {
    return OBJECT_MAPPER.readValue(inputStream, ReportConfig.class);
  }

  public String encode() throws IOException
  {
    return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

}
