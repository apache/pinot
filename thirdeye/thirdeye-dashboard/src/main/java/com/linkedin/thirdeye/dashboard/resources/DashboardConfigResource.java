package com.linkedin.thirdeye.dashboard.resources;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Future;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.math3.util.Pair;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.client.CollectionMapThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeRequest.ThirdEyeRequestBuilder;
import com.linkedin.thirdeye.client.util.ThirdEyeClientConfig;
import com.linkedin.thirdeye.dashboard.api.CollectionSchema;
import com.linkedin.thirdeye.dashboard.api.QueryResult;
import com.linkedin.thirdeye.dashboard.util.DataCache;
import com.linkedin.thirdeye.dashboard.util.QueryCache;
import com.linkedin.thirdeye.dashboard.util.UriUtils;

/**
 * REST controller providing general collection information such as dimensions and metrics.
 * @author jteoh
 */
@Path("/dashboardConfig")
@Produces(MediaType.APPLICATION_JSON)
public class DashboardConfigResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(DashboardConfigResource.class);
  private static final String DIMENSION_VALUES_OPTIONS_METRIC_FUNCTION = "AGGREGATE_1_HOURS(%s)";
  private static final String DIMENSION_VALUES_OPTIONS_THRESHOLD = "0.01";
  private static final String DIMENSION_VALUES_LIMIT = "25";
  private static final TypeReference<List<String>> LIST_TYPE_REF =
      new TypeReference<List<String>>() {
      };
  private static final Joiner METRIC_FUNCTION_JOINER = Joiner.on(",");

  private final DataCache dataCache;
  private final QueryCache queryCache;
  private final ObjectMapper objectMapper;
  private final CollectionMapThirdEyeClient clientMap;
  private final String clientConfigFolder;
  private final FunnelsDataProvider funnelsProvider;

  public DashboardConfigResource(DataCache dataCache, QueryCache queryCache,
      CollectionMapThirdEyeClient clientMap, String clientConfigFilePath,
      FunnelsDataProvider funnelsProvider, ObjectMapper objectMapper) {
    this.dataCache = dataCache;
    this.queryCache = queryCache;
    this.clientMap = clientMap;
    this.clientConfigFolder = clientConfigFilePath;
    this.funnelsProvider = funnelsProvider;
    this.objectMapper = objectMapper;
  }

  public List<String> getAllDimensions(String collection) throws Exception {
    Multimap<String, String> nul = null;
    return getDimensions(collection, nul);
  }

  /** Return a sorted list of dimensions for the provided collection. */
  @GET
  @Path("/dimensions/{collection}")
  public List<String> getDimensions(@PathParam("collection") String collection,
      @Context UriInfo uriInfo) throws Exception {
    return getDimensions(collection, UriUtils.extractDimensionValues(uriInfo));
  }

  public List<String> getDimensions(String collection,
      Multimap<String, String> selectedDimensionValues) throws Exception {
    CollectionSchema schema = dataCache.getCollectionSchema(collection);
    Set<String> selectedDimensions =
        (selectedDimensionValues == null ? null : selectedDimensionValues.keySet());
    if (CollectionUtils.isEmpty(selectedDimensions)) {
      return schema.getDimensions();
    }
    List<String> filteredDimensions =
        new ArrayList<>(CollectionUtils.subtract(schema.getDimensions(), selectedDimensions));
    Collections.sort(filteredDimensions);
    return filteredDimensions;
  }

  /**
   * Return a sorted map of dimension -> aliases for the provided collection. If no alias exists,
   * 'null' will be returned in the JSON response. This method does not filter out pre-selected
   * dimensions.
   */

  @GET
  @Path("/dimensionAliases/{collection}")
  public Map<String, String> getDimensionAliases(@PathParam("collection") String collection)
      throws Exception {
    CollectionSchema schema = dataCache.getCollectionSchema(collection);
    Iterator<String> dimensionsIter = schema.getDimensions().iterator();
    Iterator<String> dimensionAliasesIter = schema.getDimensionAliases().iterator();
    Map<String, String> aliasMap = new TreeMap<>();
    while (dimensionsIter.hasNext()) {
      String dimension = dimensionsIter.next();
      String dimensionAlias = dimensionAliasesIter.next();
      aliasMap.put(dimension, dimensionAlias);
    }
    return aliasMap;

  }

  /**
   * Returns a map of dimensions -> list of observed dimension values in the given time
   * window, sorted in decreasing order of overall contribution. Any items not meeting a
   * contribution threshold of {@value #DIMENSION_VALUES_OPTIONS_THRESHOLD} for at least one metric
   * are omitted, and a
   * maximum of {@value #DIMENSION_VALUES_LIMIT} results are returned for each metric and any given
   * dimension.
   * </br>
   * Baseline/current are provided to limit the range in which to search for dimension values.
   * </br>
   * This method currently does not take into account already selected dimensions.
   */
  @GET
  @Path("/dimensionValues/{collection}/{baseline}/{current}")
  public Map<String, Collection<String>> getDimensionValues(
      @PathParam("collection") String collection, @PathParam("baseline") long baselineMillis,
      @PathParam("current") long currentMillis,
      @DefaultValue(DIMENSION_VALUES_OPTIONS_THRESHOLD) @QueryParam("threshold") double contributionThreshold,
      @DefaultValue(DIMENSION_VALUES_LIMIT) @QueryParam("limit") int dimensionValuesLimit)
      throws Exception {
    return retrieveDimensionValues(collection, baselineMillis, currentMillis, contributionThreshold,
        dimensionValuesLimit);
  }

  public Map<String, Collection<String>> getDimensionValues(String collection, long baselineMillis,
      long currentMillis) throws NumberFormatException, Exception {
    return getDimensionValues(collection, baselineMillis, currentMillis,
        Double.valueOf(DIMENSION_VALUES_OPTIONS_THRESHOLD),
        Integer.valueOf(DIMENSION_VALUES_LIMIT));
  }

  /** Return a list of metrics for the provided collection. */
  @GET
  @Path("/metrics/{collection}")
  public List<String> getMetrics(@PathParam("collection") String collection) throws Exception {
    CollectionSchema schema = dataCache.getCollectionSchema(collection);
    return schema.getMetrics();
  }

  /**
   * Return a list of metric aliases for the provided collection. If no alias exists, 'null' will
   * be returned in the JSON response.
   */
  @GET
  @Path("/metricAliases/{collection}")
  public Map<String, String> getMetricAliases(@PathParam("collection") String collection)
      throws Exception {
    CollectionSchema schema = dataCache.getCollectionSchema(collection);
    List<String> metricAliases = schema.getMetricAliases();

    Iterator<String> metricsIter = getMetrics(collection).iterator();
    Iterator<String> metricAliasesIter = metricAliases.iterator();
    Map<String, String> aliasMap = new TreeMap<>();
    while (metricsIter.hasNext()) {
      String metric = metricsIter.next();
      String metricAlias = metricAliasesIter.next();
      aliasMap.put(metric, metricAlias);
    }

    return aliasMap;
  }

  public List<String> getMetricAliasesAsList(String collection) throws Exception {
    CollectionSchema schema = dataCache.getCollectionSchema(collection);
    return schema.getMetricAliases();
  }

  // TODO pre-cache dimension values/write to file.
  // Ignored already selected dimensions
  private Map<String, Collection<String>> retrieveDimensionValues(String collection,
      long baselineMillis, long currentMillis, double contributionThreshold,
      int dimensionValuesLimit) throws Exception {
    List<String> dimensions = getAllDimensions(collection);
    DateTime baseline = new DateTime(baselineMillis);
    DateTime current = new DateTime(currentMillis);

    List<String> metrics = getMetrics(collection);
    String dummyFunction = String.format(DIMENSION_VALUES_OPTIONS_METRIC_FUNCTION,
        METRIC_FUNCTION_JOINER.join(metrics));

    Multimap<String, String> dimensionValues = LinkedListMultimap.create();
    Map<String, List<Future<QueryResult>>> resultFutures = new HashMap<>();
    // query w/ group by for each dimension.
    for (String dimension : dimensions) {
      ArrayList<Future<QueryResult>> futures = new ArrayList<>();

      // Generate requests
      ThirdEyeRequest req1 = new ThirdEyeRequestBuilder().setCollection(collection)
          .setMetricFunction(dummyFunction).setStartTimeInclusive(baseline).setEndTime(baseline.plusDays(1))
          .setDimensionValues(dimensionValues).setGroupBy(dimension).setShouldGroupByTime(false)
          .build();
      LOGGER.info("Generated request for dimension retrieval: {}", req1);
      futures.add(queryCache.getQueryResultAsync(req1));

      ThirdEyeRequest req2 = new ThirdEyeRequestBuilder().setCollection(collection)
          .setMetricFunction(dummyFunction).setStartTimeInclusive(current.minusDays(1)).setEndTime(current)
          .setDimensionValues(dimensionValues).setGroupBy(dimension).setShouldGroupByTime(false)
          .build();
      LOGGER.info("Generated request for dimension retrieval: {}", req2);
      futures.add(queryCache.getQueryResultAsync(req2));

      // Query (in parallel)
      resultFutures.put(dimension, futures);
    }

    Map<String, Collection<String>> collectedDimensionValues = new HashMap<>();
    // Wait for all queries and generate the ordered list from the result.
    for (int i = 0; i < dimensions.size(); i++) {
      String dimension = dimensions.get(i);

      // Sum up hourly data over entire dataset for each dimension combination
      int metricCount = metrics.size();
      double[] total = new double[metricCount];
      Map<String, double[]> summedValues = new HashMap<>();
      QueryResult mergedQueryResult = new QueryResult();

      boolean inited = false;
      for (Future<QueryResult> futureQueryResult : resultFutures.get(dimension)) {
        QueryResult queryResult = futureQueryResult.get();
        if (!inited) {
          mergedQueryResult.setDimensions(queryResult.getDimensions());
          mergedQueryResult.setMetrics(queryResult.getMetrics());
          mergedQueryResult.setData(new HashMap<>(queryResult.getData()));
          inited = true;
        } else {
          for (String dimValue : queryResult.getData().keySet()) {
            if (mergedQueryResult.getData().containsKey(dimValue)) {
              mergedQueryResult.getData().get(dimValue).putAll(queryResult.getData().get(dimValue));
            }
            mergedQueryResult.getData().put(dimValue, queryResult.getData().get(dimValue));
          }
        }
      }

      QueryResult queryResult = mergedQueryResult;

      for (Map.Entry<String, Map<String, Number[]>> entry : queryResult.getData().entrySet()) {
        double[] sum = new double[metricCount];

        for (Map.Entry<String, Number[]> hourlyEntry : entry.getValue().entrySet()) {
          for (int j = 0; j < metricCount; j++) {
            double value = hourlyEntry.getValue()[j].doubleValue();
            sum[j] += value;
          }
        }
        summedValues.put(entry.getKey(), sum);

        // update total w/ sums for each dimension value.
        for (int j = 0; j < metricCount; j++) {
          total[j] += sum[j];
        }

      }

      // compare by value ascending (want poll to remove smallest element)
      List<PriorityQueue<Pair<String, Double>>> topNValuesByMetric =
          new ArrayList<PriorityQueue<Pair<String, Double>>>(metricCount);
      double[] threshold = new double[metricCount];
      Comparator<Pair<String, Double>> valueComparator = new Comparator<Pair<String, Double>>() {
        @Override
        public int compare(Pair<String, Double> a, Pair<String, Double> b) {
          return Double.compare(a.getValue().doubleValue(), b.getValue().doubleValue());
        }
      };
      for (int j = 0; j < metricCount; j++) {
        threshold[j] = total[j] * contributionThreshold;
        topNValuesByMetric.add(new PriorityQueue<>(dimensionValuesLimit, valueComparator));
      }

      // For each dimension value, add it only if it meets the threshold and drop an element from
      // the priority queue if over the limit.
      for (Map.Entry<String, double[]> entry : summedValues.entrySet()) {
        List<String> combination = objectMapper.readValue(entry.getKey(), LIST_TYPE_REF);
        String dimensionValue = combination.get(i);
        for (int j = 0; j < metricCount; j++) { // metricCount == entry.getValue().length
          double dimensionValueContribution = entry.getValue()[j];
          if (dimensionValueContribution >= threshold[j]) {
            PriorityQueue<Pair<String, Double>> topNValues = topNValuesByMetric.get(j);
            topNValues.add(new Pair<>(dimensionValue, dimensionValueContribution));
            if (topNValues.size() > dimensionValuesLimit) {
              topNValues.poll();
            }
          }
        }
      }

      // Poll returns the elements in order of ascending contribution, so poll and reverse the
      // order.

      // not LinkedHashSet because we need to reverse insertion order with metrics.
      List<String> sortedValues = new ArrayList<>();
      HashSet<String> sortedValuesSet = new HashSet<>();

      for (int j = 0; j < metricCount; j++) {
        PriorityQueue<Pair<String, Double>> topNValues = topNValuesByMetric.get(j);
        int startIndex = sortedValues.size();
        while (!topNValues.isEmpty()) {
          Pair<String, Double> pair = topNValues.poll();
          String dimensionValue = pair.getKey();
          if (!sortedValuesSet.contains(dimensionValue)) {
            sortedValues.add(startIndex, dimensionValue);
            sortedValuesSet.add(dimensionValue);
          }
        }
      }

      collectedDimensionValues.put(dimension, sortedValues);
    }
    return collectedDimensionValues;
  }

  /**
   * Returns a Map consisting of
   * {@link #getDimensionInfo(String, UriInfo, long, long, double, int)} and
   * {@link #getMetricInfo(String)} merged into a single JSON object.
   * @throws Exception
   */
  @GET
  @Path("/schema/{collection}/{baseline}/{current}")
  public Map<String, Object> getSchemaInfo(@PathParam("collection") String collection,
      @Context UriInfo uriInfo, @PathParam("baseline") long baselineMillis,
      @PathParam("current") long currentMillis,
      @DefaultValue(DIMENSION_VALUES_OPTIONS_THRESHOLD) @QueryParam("threshold") double contributionThreshold,
      @DefaultValue(DIMENSION_VALUES_LIMIT) @QueryParam("limit") int dimensionValuesLimit)
      throws Exception {
    Map<String, Object> schemaInfo = new HashMap<>();
    schemaInfo.putAll(getDimensionInfo(collection, uriInfo, baselineMillis, currentMillis,
        contributionThreshold, dimensionValuesLimit));
    schemaInfo.putAll(getMetricInfo(collection));
    return schemaInfo;
  }

  /**
   * Returns a Map consisting of:</br>
   * <ul>
   * <li>Dimensions ({@link #getDimensions(String, UriInfo)}}
   * <li>Dimension Aliases ({@link #getDimensionAliases(String)}
   * </li>Dimension Values ({@link #getDimensionValues(String, long, long)}
   * </ul>
   * These values are mapped to their lowerCamelCase keys in an associative array.
   * @throws Exception
   */
  @GET
  @Path("/dimensionInfo/{collection}/{baseline}/{current}")
  public Map<String, Object> getDimensionInfo(@PathParam("collection") String collection,
      @Context UriInfo uriInfo, @PathParam("baseline") long baselineMillis,
      @PathParam("current") long currentMillis,
      @DefaultValue(DIMENSION_VALUES_OPTIONS_THRESHOLD) @QueryParam("threshold") double contributionThreshold,
      @DefaultValue(DIMENSION_VALUES_LIMIT) @QueryParam("limit") int dimensionValuesLimit)
      throws Exception {
    Map<String, Object> dimensionInfo = new HashMap<>();
    dimensionInfo.put("dimensions", getDimensions(collection, uriInfo));
    dimensionInfo.put("dimensionAliases", getDimensionAliases(collection));
    dimensionInfo.put("dimensionValues", getDimensionValues(collection, baselineMillis,
        currentMillis, contributionThreshold, dimensionValuesLimit));
    return dimensionInfo;
  }

  /**
   * Returns a Map consisting of:</br>
   * <ul>
   * <li>Metrics ({@link #getMetrics(String)})
   * <li>Metric Aliases ({@link #getMetricAliasesAsList(String)})
   * </ul>
   * These values are mapped to their lowerCamelCase keys in an associative array.
   * @throws Exception
   */
  @GET
  @Path("/metricInfo/{collection}")
  public Map<String, Object> getMetricInfo(@PathParam("collection") String collection)
      throws Exception {
    Map<String, Object> schemaInfo = new HashMap<>();
    schemaInfo.put("metrics", getMetrics(collection));
    schemaInfo.put("metricAliases", getMetricAliasesAsList(collection));
    return schemaInfo;
  }

  /**
   * Returns all collections for this dashboard instance
   * @throws Exception
   */
  @GET
  @Path("/collections/")
  public List<String> getCollections() throws Exception {
    return dataCache.getCollections();
  }

  /**
   * Reloads client configurations.
   * @throws Exception
   */
  @POST
  @Path("/reloadClients")
  public Response reloadClients() throws Exception {
    LOGGER.info("Reloading client configurations from {}", clientConfigFolder);
    clientMap.reloadFromFolder(clientConfigFolder);
    if (funnelsProvider != null) {
      funnelsProvider.loadConfigs();
    }
    return Response.ok().build();
  }

  @GET
  @Path("/clientConfigs")
  public List<ThirdEyeClientConfig> getClientConfigs() throws Exception {
    return clientMap.getClientConfigs();
  }

}
