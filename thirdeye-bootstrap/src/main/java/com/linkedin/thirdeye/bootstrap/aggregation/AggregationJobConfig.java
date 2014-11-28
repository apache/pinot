package com.linkedin.thirdeye.bootstrap.aggregation;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.map.ObjectMapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream;

public class AggregationJobConfig {
  private List<String> dimensionNames;
  private List<String> metricNames;
  private List<String> metricTypes;
  private String timeColumnName;
  private String timeUnit;
  private String aggregationGranularity;

  public AggregationJobConfig() {

  }

  public AggregationJobConfig(List<String> dimensionNames,
      List<String> metricNames, List<String> metricTypes,
      String timeColumnName, String timeUnit, String aggregationGranularity) {
    super();
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.metricTypes = metricTypes;
    this.timeColumnName = timeColumnName;
    this.timeUnit = timeUnit;
    this.aggregationGranularity = aggregationGranularity;
  }

  public List<String> getDimensionNames() {
    return dimensionNames;
  }

  public List<String> getMetricNames() {
    return metricNames;
  }

  public List<String> getMetricTypes() {
    return metricTypes;
  }

  public String getTimeColumnName() {
    return timeColumnName;
  }

  public String getTimeUnit() {
    return timeUnit;
  }

  public String getAggregationGranularity() {
    return aggregationGranularity;
  }

  public static AggregationJobConfig fromJson(JsonNode readTree) {
    ObjectMapper objectMapper = new ObjectMapper();

    return null;
  }

  public static void main(String[] args) throws Exception {
    // "numberOfImportedContacts": 17, "numberOfSuggestedMemberConnections": 17,
    // "numberOfMemberConnectionsSent": 17, "numberOfSuggestedGuestInvitations":
    // 104, "numberOfGuestInvitationsSent": 104}
    List<String> dimensionNames = Lists.newArrayList("country", "browser",
        "emailDomain", "device", "source");
    List<String> metricNames = Lists.newArrayList("numberOfImportedContacts",
        "numberOfSuggestedMemberConnections", "numberOfMemberConnectionsSent",
        "numberOfSuggestedGuestInvitations", "numberOfGuestInvitationsSent");
    List<String> metricTypes = Lists.newArrayList("int", "int", "int", "int",
        "int");
    String timeColumnName = "hoursSinceEpoch";
    String timeUnit = TimeUnit.HOURS.toString();
    String aggregationGranularity = TimeUnit.HOURS.toString();
    ;
    AggregationJobConfig config = new AggregationJobConfig(dimensionNames,
        metricNames, metricTypes, timeColumnName, timeUnit,
        aggregationGranularity);
    ByteOutputStream out = new ByteOutputStream();
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.defaultPrettyPrintingWriter().writeValue(out, config);
    String x = new String(out.getBytes());
    System.out.println(x);

    AggregationJobConfig readValue = objectMapper.readValue(x.getBytes(),
        AggregationJobConfig.class);
System.out.println(readValue);
  }
}
