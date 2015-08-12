package com.linkedin.thirdeye.reporting.api;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.thirdeye.anomaly.api.AnomalyDatabaseConfig;

public class ReportConfig {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

  @NotNull
  private String name;
  @NotNull
  private String collection;
  @NotNull
  private List<TableSpec> tables;
  private AliasSpec aliases;
  private DateTime endTime;
  private DateTime startTime;
  private String startTimeString;
  private String endTimeString;

  private Map<String, ScheduleSpec> schedules;
  private DBSpec dbconfig;
  @NotNull
  private String timezone;


  public ReportConfig() {

  }



  public String getStartTimeString() {
    return startTimeString;
  }



  public void setStartTimeString(String startTimeString) {
    this.startTimeString = startTimeString;
  }



  public String getEndTimeString() {
    return endTimeString;
  }



  public void setEndTimeString(String endTimeString) {
    this.endTimeString = endTimeString;
  }



  public DBSpec getDbconfig() {
    return dbconfig;
  }


  public void setDbconfig(DBSpec dbconfig) {
    this.dbconfig = dbconfig;
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

  public String getTimezone() {
    return timezone;
  }

  public void setTimezone(String timezone) {
    this.timezone = timezone;
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
