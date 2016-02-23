package com.linkedin.thirdeye.dashboard.views;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;

import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dashboard.resources.DashboardConfigResource;

import io.dropwizard.views.View;

public class DashboardView extends View {
  private final String collection;
  private final List<String> dimensions;
  private final List<String> unselectedDimensions;
  private final Map<String, String> aliases;
  private final List<String> metrics;
  private final List<String> metricAliases;
  private final Multimap<String, String> selectedDimensions;
  private final DateTime baselineTime;
  private final DateTime currentTime;
  private final DimensionView dimensionView;
  private final DateTime earliestDataTime;
  private final DateTime latestDataTime;
  private final List<String> customDashboardNames;
  private final String feedbackEmailAddress;
  private final List<String> funnelNames;

  public DashboardView(String collection, Multimap<String, String> selectedDimensions,
      DateTime baselineTime, DateTime currentTime, DimensionView dimensionView,
      DateTime earliestDataTime, DateTime latestDataTime, List<String> customDashboardNames,
      String feedbackEmailAddress, List<String> allFunnelNames,
      DashboardConfigResource dashboardConfig) throws Exception {
    super("dashboard.ftl");
    this.collection = collection;
    this.selectedDimensions = selectedDimensions;
    this.feedbackEmailAddress = feedbackEmailAddress;
    this.baselineTime = baselineTime;
    this.currentTime = currentTime;
    this.dimensionView = dimensionView;
    this.earliestDataTime = earliestDataTime;
    this.latestDataTime = latestDataTime;
    this.customDashboardNames = customDashboardNames;
    this.funnelNames = allFunnelNames;

    // TODO delete these values once the UI is decoupled.
    this.dimensions = dashboardConfig.getAllDimensions(collection);
    this.unselectedDimensions = dashboardConfig.getDimensions(collection, selectedDimensions);
    this.aliases = dashboardConfig.getDimensionAliases(collection);
    this.metrics = dashboardConfig.getMetrics(collection);
    this.metricAliases = dashboardConfig.getMetricAliasesAsList(collection);
  }

  public List<String> getFunnelNames() {
    return funnelNames;
  }

  public String getCollection() {
    return collection;
  }

  public Map<String, String> getSelectedDimensions() {
    Map<String, String> dimensions = new HashMap<>(selectedDimensions.size());
    for (String key : selectedDimensions.keySet()) {
      Collection<String> values = selectedDimensions.get(key);
      String value = StringUtils.join(values, " OR ");
      dimensions.put(key, value);
    }

    return dimensions;
  }

  public List<String> getUnselectedDimensions() throws Exception {
    return unselectedDimensions;
  }

  public String getFeedbackEmailAddress() {
    return feedbackEmailAddress;
  }

  public List<String> getDimensions() {
    return dimensions;
  }

  public Map<String, String> getDimensionAliases() {
    return aliases;
  }

  public List<String> getMetrics() {
    return metrics;
  }

  public List<String> getMetricAliases() {
    return metricAliases;
  }

  public DateTime getBaselineTime() {
    return baselineTime;
  }

  public DateTime getCurrentTime() {
    return currentTime;
  }

  public DimensionView getDimensionView() {
    return dimensionView;
  }

  public DateTime getEarliestDataTime() {
    return earliestDataTime;
  }

  public DateTime getLatestDataTime() {
    return latestDataTime;
  }

  public List<String> getCustomDashboardNames() {
    return customDashboardNames;
  }

}
