package com.linkedin.thirdeye.dashboard.views;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;

import com.linkedin.thirdeye.dashboard.api.CollectionSchema;

import io.dropwizard.views.View;

public class DashboardView extends View {
  private final String collection;
  private final CollectionSchema collectionSchema;
  private final List<String> dimensions;
  private final Map<String, String> aliases;
  private final MultivaluedMap<String, String> selectedDimensions;
  private final DateTime baselineTime;
  private final DateTime currentTime;
  private final DimensionView dimensionView;
  private final DateTime earliestDataTime;
  private final DateTime latestDataTime;
  private final List<String> customDashboardNames;
  private final String feedbackEmailAddress;
  private final List<String> funnelNames;

  public DashboardView(String collection, CollectionSchema collectionSchema,
      MultivaluedMap<String, String> selectedDimensions, DateTime baselineTime,
      DateTime currentTime, DimensionView dimensionView, DateTime earliestDataTime,
      DateTime latestDataTime, List<String> customDashboardNames, String feedbackEmailAddress,
      List<String> allFunnelNames) {
    super("dashboard.ftl");
    this.collection = collection;
    this.selectedDimensions = selectedDimensions;
    this.feedbackEmailAddress = feedbackEmailAddress;
    this.collectionSchema = collectionSchema;
    this.baselineTime = baselineTime;
    this.currentTime = currentTime;
    this.dimensionView = dimensionView;
    this.earliestDataTime = earliestDataTime;
    this.latestDataTime = latestDataTime;
    this.customDashboardNames = customDashboardNames;
    this.funnelNames = allFunnelNames;

    this.dimensions = new ArrayList<>(collectionSchema.getDimensions());
    Collections.sort(dimensions);

    this.aliases = new TreeMap<>();
    List<String> dimensions = collectionSchema.getDimensions();
    List<String> dimensionAliases = collectionSchema.getDimensionAliases();

    for (int i = 0; i < dimensions.size(); i++) {
      aliases.put(dimensions.get(i), dimensionAliases.get(i));
    }
  }

  public List<String> getFunnelNames() {
    return funnelNames;
  }

  public String getCollection() {
    return collection;
  }

  public Map<String, String> getSelectedDimensions() {
    Map<String, String> dimensions = new HashMap<>(selectedDimensions.size());
    for (Map.Entry<String, List<String>> entry : selectedDimensions.entrySet()) {
      String value = StringUtils.join(entry.getValue(), " OR ");
      dimensions.put(entry.getKey(), value);
    }
    return dimensions;
  }

  public String getFeedbackEmailAddress() {
    return feedbackEmailAddress;
  }

  public CollectionSchema getCollectionSchema() {
    return collectionSchema;
  }

  public List<String> getDimensions() {
    return dimensions;
  }

  public Map<String, String> getDimensionAliases() {
    return aliases;
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
