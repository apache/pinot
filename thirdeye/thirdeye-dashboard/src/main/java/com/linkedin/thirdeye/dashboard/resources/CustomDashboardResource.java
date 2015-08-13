package com.linkedin.thirdeye.dashboard.resources;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Joiner;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.linkedin.thirdeye.dashboard.api.CollectionSchema;
import com.linkedin.thirdeye.dashboard.api.QueryResult;
import com.linkedin.thirdeye.dashboard.api.custom.CustomDashboardSpec;
import com.linkedin.thirdeye.dashboard.api.custom.CustomDashboardComponentSpec;
import com.linkedin.thirdeye.dashboard.util.DataCache;
import com.linkedin.thirdeye.dashboard.util.QueryCache;
import com.linkedin.thirdeye.dashboard.util.SqlUtils;
import com.linkedin.thirdeye.dashboard.util.UriUtils;
import com.linkedin.thirdeye.dashboard.views.CustomDashboardView;
import com.linkedin.thirdeye.dashboard.views.CustomFunnelTabularView;
import com.linkedin.thirdeye.dashboard.views.CustomTimeSeriesView;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import io.dropwizard.views.View;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.math3.util.Pair;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Path("/custom-dashboard")
public class CustomDashboardResource {
  private static final Logger LOG = LoggerFactory.getLogger(CustomDashboardResource.class);
  private static final Joiner METRIC_FUNCTION_JOINER = Joiner.on(",");
  private static final TypeReference<List<String>> LIST_REF = new TypeReference<List<String>>(){};
  private final File customDashboardRoot;
  private final String serverUri;
  private final QueryCache queryCache;
  private final DataCache dataCache;
  private final ObjectMapper yamlObjectMapper;
  private final ObjectMapper objectMapper;
  private final LoadingCache<CacheKey, CustomDashboardSpec> cache;

  public CustomDashboardResource(File customDashboardRoot,
                                 String serverUri,
                                 QueryCache queryCache,
                                 DataCache dataCache) {
    this.customDashboardRoot = customDashboardRoot;
    this.serverUri = serverUri;
    this.queryCache = queryCache;
    this.dataCache = dataCache;
    this.yamlObjectMapper = new ObjectMapper(new YAMLFactory());
    this.objectMapper = new ObjectMapper();
    this.cache = CacheBuilder.newBuilder()
        .expireAfterWrite(5, TimeUnit.MINUTES)
        .build(new CustomDashboardSpecLoader());
  }

  private class CustomDashboardSpecLoader extends CacheLoader<CacheKey, CustomDashboardSpec> {
    @Override
    public CustomDashboardSpec load(CacheKey key) throws Exception {
      File collectionDir = new File(customDashboardRoot, key.getCollection());
      File configFile = new File(collectionDir, key.getName());
      return yamlObjectMapper.readValue(configFile, CustomDashboardSpec.class);
    }
  }

  public List<String> getCustomDashboardNames(String collection) {
    List<String> names = null;

    File collectionDir = new File(customDashboardRoot, collection);
    if (!collectionDir.isAbsolute()) {
      throw new IllegalArgumentException("Not absolute " + collectionDir);
    }

    if (collectionDir.exists()) {
      names = Arrays.asList(collectionDir.list());
    }

    return names;
  }

  // Config management

  @POST
  @Path("/config/{collection}/{name}")
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  public Response post(
      @PathParam("collection") String collection,
      @PathParam("name") String name, byte[] config) throws Exception {
    File collectionDir = new File(customDashboardRoot, collection);
    File configFile = new File(collectionDir, name);

    // Paths should never be relative
    if (!configFile.isAbsolute()) {
      return Response.status(Response.Status.BAD_REQUEST).entity("Path is not absolute: " + configFile).build();
    }

    // Do not overwrite existing
    if (configFile.exists()) {
      return Response.status(Response.Status.CONFLICT).entity("Config already exists for " + name).build();
    }

    // Create collection dir if it doesn't exist
    if (!collectionDir.exists()) {
      FileUtils.forceMkdir(collectionDir);
      LOG.info("Created {} for custom dashboards", collectionDir);
    }

    // Write new config file
    try (OutputStream outputStream = new FileOutputStream(configFile)) {
      IOUtils.copy(new ByteArrayInputStream(config), outputStream);
      LOG.info("Created custom dashboard {} for collection {}", name, collection);
    } catch (Exception e) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Could not write to " + configFile).build();
    }

    return Response.ok().build();
  }

  @GET
  @Path("/config/{collection}/{name}")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  public Response get(@PathParam("collection") String collection, @PathParam("name") String name) {
    File collectionDir = new File(customDashboardRoot, collection);
    File configFile = new File(collectionDir, name);

    // Paths should never be relative
    if (!configFile.isAbsolute()) {
      return Response.status(Response.Status.BAD_REQUEST).entity("Path is not absolute: " + configFile).build();
    }

    // Check if exists
    if (!configFile.exists()) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    try (InputStream inputStream = new FileInputStream(configFile)) {
      byte[] config = IOUtils.toByteArray(inputStream);
      return Response.ok(config, MediaType.APPLICATION_OCTET_STREAM).build();
    } catch (Exception e) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Could not read from " + configFile).build();
    }
  }

  @DELETE
  @Path("/config/{collection}/{name}")
  public Response delete(@PathParam("collection") String collection, @PathParam("name") String name) {
    File collectionDir = new File(customDashboardRoot, collection);
    File configFile = new File(collectionDir, name);

    // Paths should never be relative
    if (!configFile.isAbsolute()) {
      return Response.status(Response.Status.BAD_REQUEST).entity("Path is not absolute: " + configFile).build();
    }

    // Check if exists
    if (!configFile.exists()) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    try {
      FileUtils.forceDelete(configFile);
      cache.invalidate(name);
      LOG.info("Deleted custom dashboard {} for collection {}", name, collection);
      return Response.noContent().build();
    } catch (Exception e) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("Could not delete " + configFile).build();
    }
  }

  // Dashboard rendering

  @GET
  @Path("/dashboard/{collection}/{name}/{year}/{month}/{day}")
  public CustomDashboardView getCustomDashboard(
      @PathParam("collection") String collection,
      @PathParam("name") String name,
      @PathParam("year") Integer year,
      @PathParam("month") Integer month,
      @PathParam("day") Integer day) throws Exception {
    CustomDashboardSpec spec = cache.get(new CacheKey(collection, name));

    // Get funnel views
    List<Pair<CustomDashboardComponentSpec, View>> views = new ArrayList<>();
    for (CustomDashboardComponentSpec componentSpec : spec.getComponents()) {
      MultivaluedMap<String, String> dimensionValues = new MultivaluedMapImpl();
      if (componentSpec.getDimensions() != null) {
        for (Map.Entry<String, List<String>> entry : componentSpec.getDimensions().entrySet()) {
          dimensionValues.put(entry.getKey(), entry.getValue());
        }
      }

      List<String> metrics = componentSpec.getMetrics();
      String groupBy = componentSpec.getGroupBy();

      View view = null;
      switch (componentSpec.getType()) {
        case FUNNEL:
          view = getCustomFunnelTabularView(collection, year, month, day, metrics, dimensionValues);
          break;
        case TIME_SERIES:
          view = getCustomTimeSeriesView(collection, year, month, day, metrics, dimensionValues, groupBy);
          break;
        default:
          throw new IllegalStateException("Invalid funnel spec " + componentSpec.getType());
      }

      if (view != null) {
        views.add(new Pair<>(componentSpec, view));
      }
    }

    return new CustomDashboardView(name, views);
  }

  private CustomTimeSeriesView getCustomTimeSeriesView(
      String collection,
      Integer year,
      Integer month,
      Integer day,
      List<String> metricList,
      MultivaluedMap<String, String> queryParams,
      String groupBy) throws Exception {
    // Always aggregate at 1 hour (for intra-day style report)
    String metricFunction = "AGGREGATE_1_HOURS(" + METRIC_FUNCTION_JOINER.join(metricList) + ")";
    DateTime current = new DateTime(year, month, day, 0, 0);
    DateTime baseline = current.minusWeeks(1);
    MultivaluedMap<String, String> dimensionValues = queryParams;

    if (groupBy != null) {
      if (dimensionValues.containsKey(groupBy)) {
        throw new IllegalArgumentException("Cannot group by fixed dimension");
      }
      dimensionValues.put(groupBy, Arrays.asList("!"));
    }

    // Query
    String sql = SqlUtils.getSql(metricFunction, collection, baseline, current, dimensionValues);
    QueryResult result = queryCache.getQueryResult(serverUri, sql);

    // Get index of group by so we can extract values for labels
    CollectionSchema schema = dataCache.getCollectionSchema(serverUri, collection);
    int groupByIdx = -1;
    for (int i = 0; i < schema.getDimensions().size(); i++) {
      if (schema.getDimensions().get(i).equals(groupBy)) {
        groupByIdx = i;
        break;
      }
    }

    // Compose result structure (note: only one metric for each dimension)
    Map<String, Map<Long, Number>> groupedSeries = new HashMap<>();
    final Map<String, Long> aggregates = new HashMap<>();

    for (Map.Entry<String, Map<String, Number[]>> entry : result.getData().entrySet()) {
      // Dimension value
      String dimensionValue = "";
      if (groupByIdx >= 0) {
        List<String> values = objectMapper.readValue(entry.getKey(), LIST_REF);
        dimensionValue = values.get(groupByIdx);
      }

      // Time series
      Map<Long, Number> series = new HashMap<>();
      aggregates.put(dimensionValue, 0L);
      groupedSeries.put(dimensionValue, series);
      for (Map.Entry<String, Number[]> dataPoint : entry.getValue().entrySet()) {
        Long time = Long.valueOf(dataPoint.getKey());
        Number value = dataPoint.getValue()[0];
        series.put(time, value);
        aggregates.put(dimensionValue, aggregates.get(dimensionValue) + value.longValue());
      }
    }

    // Pick top 5 based on whole series
    List<String> chosenValues = new ArrayList<>(groupedSeries.keySet());
    Collections.sort(chosenValues, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        return (int) (aggregates.get(o2) - aggregates.get(o1)); // reverse
      }
    });
    if (chosenValues.size() > 5) {
      chosenValues = chosenValues.subList(0, 5);
    }

    // Get times
    List<Long> times = new ArrayList<>();
    DateTime cursor = new DateTime(baseline.getMillis());
    while (cursor.compareTo(current) < 0) {
      times.add(cursor.getMillis());
      cursor = cursor.plusHours(1);
    }

    // Get data
    Map<String, List<Number[]>> allSeries = new HashMap<>();
    for (String chosenValue : chosenValues) {
      Map<Long, Number> seriesMap = groupedSeries.get(chosenValue);
      List<Number[]> series = new ArrayList<>();
      for (Long time : times) {
        Number value = seriesMap.get(time);
        series.add(new Number[] { time, value == null ? 0 : value });
      }
      allSeries.put(chosenValue, series);
    }

    return new CustomTimeSeriesView(allSeries);
  }

  private CustomFunnelTabularView getCustomFunnelTabularView(
      String collection,
      Integer year,
      Integer month,
      Integer day,
      List<String> metricList,
      MultivaluedMap<String, String> queryParams) throws Exception {
    // Always aggregate at 1 hour (for intra-day style report)
    String metricFunction = "AGGREGATE_1_HOURS(" + METRIC_FUNCTION_JOINER.join(metricList) + ")";

    DateTime currentEnd = new DateTime(year, month, day, 0, 0);
    DateTime currentStart = currentEnd.minusDays(1);
    DateTime baselineEnd = currentEnd.minusWeeks(1);
    DateTime baselineStart = baselineEnd.minusDays(1);

    // SQL
    String baselineSql = SqlUtils.getSql(metricFunction, collection, baselineStart, baselineEnd, queryParams);
    String currentSql = SqlUtils.getSql(metricFunction, collection, currentStart, currentEnd, queryParams);

    // Query
    Future<QueryResult> baselineResult = queryCache.getQueryResultAsync(serverUri, baselineSql);
    Future<QueryResult> currentResult = queryCache.getQueryResultAsync(serverUri, currentSql);

    // Baseline data
    Map<Long, Number[]> baselineData = extractFunnelData(baselineResult.get());
    Map<Long, Number[]> currentData = extractFunnelData(currentResult.get());

    // Compose result
    List<Pair<Long, Number[]>> table = new ArrayList<>();
    DateTime currentCursor = new DateTime(currentStart.getMillis());
    DateTime baselineCursor = new DateTime(baselineStart.getMillis());
    while (currentCursor.compareTo(currentEnd) < 0 && baselineCursor.compareTo(baselineEnd) < 0) {
      // Get values for this time
      Number[] baselineValues = baselineData.get(baselineCursor.getMillis());
      Number[] currentValues = currentData.get(currentCursor.getMillis());
      long hourOfDay = currentCursor.getHourOfDay(); // same as baseline

      if (baselineValues == null || currentValues == null) {
        table.add(new Pair<Long, Number[]>(hourOfDay, null));
      } else {
        // Compute percent change
        Number[] change = new Number[baselineValues.length];
        for (int i = 0; i < baselineValues.length; i++) {
          if (baselineValues[i] == null || currentValues[i] == null || baselineValues[i].doubleValue() == 0.0) {
            change[i] = null; // i.e. N/A, or cannot compute ratio to baseline
          } else {
            change[i] = (currentValues[i].doubleValue() - baselineValues[i].doubleValue()) / baselineValues[i].doubleValue();
          }
        }

        // Store in table
        table.add(new Pair<>(hourOfDay, change));
      }

      // Increment
      currentCursor = currentCursor.plusHours(1);
      baselineCursor = baselineCursor.plusHours(1);
    }

    // Get mapping of metric name to index
    Map<String, Integer> metricNameToIndex = new HashMap<>();
    List<String> resultMetrics = baselineResult.get().getMetrics();
    for (int i = 0; i < resultMetrics.size(); i++) {
      metricNameToIndex.put(resultMetrics.get(i), i);
    }

    // Filter (since query result set will contain primitive metrics for each derived one)
    List<Pair<Long, Number[]>> filteredTable = new ArrayList<>();
    for (Pair<Long, Number[]> pair : table) {
      Number[] filtered = new Number[metricList.size()];
      for (int i = 0; i < metricList.size(); i++) {
        String metricName = metricList.get(i);
        Integer metricIdx = metricNameToIndex.get(metricName);
        if (pair.getSecond() == null) {
          filtered[i] = 0;
        } else {
          Number value = pair.getSecond()[metricIdx];
          filtered[i] = value;
        }
      }
      filteredTable.add(new Pair<>(pair.getFirst(), filtered));
    }

    return new CustomFunnelTabularView(metricList, filteredTable);
  }

  private static Map<Long, Number[]> extractFunnelData(QueryResult queryResult) throws Exception {
    Map<Long, Number[]> data = new HashMap<>();

    if (queryResult.getData().size() != 1) {
      throw new WebApplicationException(
          new Exception("Custom funnel tabular view cannot support multi-dimensional queries"),
          Response.Status.BAD_REQUEST);
    }

    Map.Entry<String, Map<String, Number[]>> first = queryResult.getData().entrySet().iterator().next();

    for (Map.Entry<String, Number[]> entry : first.getValue().entrySet()) {
      data.put(Long.valueOf(entry.getKey()), entry.getValue());
    }

    return data;
  }

  private static class CacheKey {
    private final String collection;
    private final String name;

    CacheKey(String collection, String name) {
      this.collection = collection;
      this.name = name;
    }

    String getCollection() {
      return collection;
    }

    String getName() {
      return name;
    }

    @Override
    public int hashCode() {
      return Objects.hash(collection, name);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof CacheKey)) {
        return false;
      }
      CacheKey k = (CacheKey) o;
      return k.getName().equals(name) && k.getCollection().equals(collection);
    }
  }
}
