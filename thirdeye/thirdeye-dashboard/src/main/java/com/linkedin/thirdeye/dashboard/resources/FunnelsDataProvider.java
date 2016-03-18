package com.linkedin.thirdeye.dashboard.resources;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Joiner;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeRequest.ThirdEyeRequestBuilder;
import com.linkedin.thirdeye.client.ThirdEyeRequestUtils;
import com.linkedin.thirdeye.dashboard.api.CollectionSchema;
import com.linkedin.thirdeye.dashboard.api.DimensionGroupSpec;
import com.linkedin.thirdeye.dashboard.api.MetricDataRow;
import com.linkedin.thirdeye.dashboard.api.MetricTable;
import com.linkedin.thirdeye.dashboard.api.QueryResult;
import com.linkedin.thirdeye.dashboard.api.funnel.CustomFunnelSpec;
import com.linkedin.thirdeye.dashboard.api.funnel.FunnelSpec;
import com.linkedin.thirdeye.dashboard.util.DataCache;
import com.linkedin.thirdeye.dashboard.util.IntraPeriod;
import com.linkedin.thirdeye.dashboard.util.QueryCache;
import com.linkedin.thirdeye.dashboard.util.ViewUtils;
import com.linkedin.thirdeye.dashboard.views.FunnelTable;
import com.sun.jersey.api.ConflictException;
import com.sun.jersey.api.NotFoundException;

@Path("/funnels")
@Produces(MediaType.APPLICATION_JSON)
public class FunnelsDataProvider {
  private static final String DEFAULT_FUNNEL_NAME = "Primary_Metric_View";

  private static final String DEFAULT_FUNNEL_VISUALIZATION_TYPE = "HEATMAP";

  private static String FUNNELS_CONFIG_FILE_NAME = "funnels.yml";
  private static String FUNNELS_CONFIG_FILE_ARCHIVE = "funnels.archive";

  private static final Logger LOG = LoggerFactory.getLogger(FunnelsDataProvider.class);
  private static final Joiner METRIC_FUNCTION_JOINER = Joiner.on(",");
  private static final TypeReference<List<String>> LIST_REF = new TypeReference<List<String>>() {
  };
  private final File funnelsRoot;
  private final QueryCache queryCache;
  private final DataCache dataCache;
  private final Map<String, CustomFunnelSpec> funnelSpecsMap;

  public FunnelsDataProvider(File funnelsRoot, QueryCache queryCache, DataCache dataCache)
      throws Exception {
    this.funnelsRoot = funnelsRoot;
    this.queryCache = queryCache;
    this.dataCache = dataCache;
    this.funnelSpecsMap = new HashMap<String, CustomFunnelSpec>();
    loadConfigs();
    LOG.info("loaded custom funnel configs with {} ",
        new ObjectMapper().writeValueAsString(funnelSpecsMap));
  }

  public CustomFunnelSpec getFunnelSpecFor(String collection) {
    return funnelSpecsMap.get(collection);
  }

  @DELETE
  @Path("/{collection}")
  public Response deleteFunnel(@PathParam("collection") String collection) throws Exception {
    File collectionDir = new File(funnelsRoot, collection);
    File funnelsConfigFile = new File(collectionDir, FUNNELS_CONFIG_FILE_NAME);

    if (funnelsConfigFile.exists()) {

      File funnelsConfigArchive = new File(collectionDir, FUNNELS_CONFIG_FILE_ARCHIVE);
      if (funnelsConfigArchive.exists()) {
        FileUtils.deleteQuietly(funnelsConfigArchive);
      }
      FileUtils.moveFile(funnelsConfigFile, funnelsConfigArchive);
    } else {
      throw new NotFoundException("Funnel for collection " + collection + " at "
          + collectionDir.getPath() + " does not exist");
    }

    loadConfigs();

    return Response.noContent().build();
  }

  @POST
  @Path("/{collection}")
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  public Response postFunnelConfig(@PathParam("collection") String collection,
      byte[] funnelConfigBytes) throws Exception {
    File collectionDir = new File(funnelsRoot, collection);
    if (!collectionDir.exists()) {
      FileUtils.forceMkdir(collectionDir);
    }

    File funnelConfigFile = new File(collectionDir, FUNNELS_CONFIG_FILE_NAME);

    if (!funnelConfigFile.exists()) {
      IOUtils.copy(new ByteArrayInputStream(funnelConfigBytes),
          new FileOutputStream(funnelConfigFile));
    } else {
      throw new ConflictException(funnelConfigFile.getPath()
          + " already exists. A DELETE of /funnels/{collection} is required first");
    }

    loadConfigs();

    return Response.ok().build();
  }

  /**
   * Computes funnel views using the provided metric function w/ the metrics replaced for each of
   * the selected funnels. Baseline and current times are used solely for their dates (ie hours are
   * excluded) and are treated as the start of the desired day. Since the client timezone is not
   * provided, start of day is defined as 12AM in America/Los_Angeles.
   * dimensionValuesMap specifies fixed dimension values for the query.
   */
  public List<FunnelTable> computeFunnelViews(String collection, String metricFunction,
      String selectedFunnels, long baselineMillis, long currentMillis,
      Multimap<String, String> dimensionValues) throws Exception {
    IntraPeriod intraPeriod = ViewUtils.getIntraPeriod(metricFunction);

    List<FunnelTable> funnelViews = new ArrayList<FunnelTable>();
    boolean defaultFunnelSelected = false;
    if (selectedFunnels != null && selectedFunnels.length() > 0) {
      String[] funnels = selectedFunnels.split(",");
      if (funnels.length == 0) {
        return null;
      }

      for (String funnel : funnels) {
        if (DEFAULT_FUNNEL_NAME.equals(funnel)) {
          defaultFunnelSelected = true;
        }
        LOG.info("adding funnel views for collection, {}, with funnel name {}", collection, funnel);
        FunnelSpec spec = funnelSpecsMap.get(collection).getFunnels().get(funnel);
        funnelViews.add(getFunnelDataFor(collection, metricFunction, spec, baselineMillis,
            currentMillis, intraPeriod, dimensionValues));
      }
    }
    if (!defaultFunnelSelected) {
      LOG.info("adding primary funnel views for collection, {}", collection);
      FunnelSpec defaultSpec = funnelSpecsMap.get(collection).getFunnels().get(DEFAULT_FUNNEL_NAME);
      funnelViews.add(getFunnelDataFor(collection, metricFunction, defaultSpec, baselineMillis,
          currentMillis, intraPeriod, dimensionValues));
    }
    return funnelViews;
  }

  @GET
  @Path("/names/{collection}")
  public List<String> getFunnelNamesFor(@PathParam("collection") String collection) {
    List<String> funnelNames = new ArrayList<String>();

    if (!funnelSpecsMap.containsKey(collection)) {
      return funnelNames;
    }

    for (FunnelSpec spec : funnelSpecsMap.get(collection).getFunnels().values()) {
      funnelNames.add(spec.getName());
    }

    return funnelNames;
  }

  @GET
  @Path("/{collection}")
  public Map<String, FunnelSpec> getFunnelsFor(@PathParam("collection") String collection) {
    if (!funnelSpecsMap.containsKey(collection)) {
      return Collections.emptyMap();
    }
    Map<String, FunnelSpec> funnels = funnelSpecsMap.get(collection).getFunnels();

    return funnels;
  }

  // filter format will be dimName1:dimValue1;dimName2:dimValue2
  /**
   * Retrieves funnel data using the provided url metric function w/ the metrics replaced by the
   * specified funnel. Baseline and current times are used solely for their dates (ie hours are
   * excluded) and are treated as the start of the desired day. Since the client timezone is not
   * provided, start of day is defined as 12AM in America/Los_Angeles.
   * dimensionValuesMap specifies fixed dimension values for the query.
   */
  public FunnelTable getFunnelDataFor(String collection, String urlMetricFunction, FunnelSpec spec,
      long baselineMillis, long currentMillis, IntraPeriod intraPeriod,
      Multimap<String, String> dimensionValuesMap) throws Exception {

    // TODO : {dpatel} : this entire flow is extremely similar to custom dashboards, we should merge
    // them

    DateTime baselineStart = ViewUtils.standardizeDate(baselineMillis, intraPeriod);
    DateTime currentStart = ViewUtils.standardizeDate(currentMillis, intraPeriod);
    long intraPeriodMillis = intraPeriod.getMillis();

    DateTime currentEnd = currentStart.plus(intraPeriodMillis);
    DateTime baselineEnd = baselineStart.plus(intraPeriodMillis);

    List<String> metricFunctionLevels = ViewUtils.getMetricFunctionLevels(urlMetricFunction);
    String metricFunction = StringUtils.join(metricFunctionLevels, "(")
        + String.format("(%s)", METRIC_FUNCTION_JOINER.join(spec.getActualMetricNames()))
        + StringUtils.repeat(")", metricFunctionLevels.size() - 1);

    Map<String, Multimap<String, String>> dimensionGroups =
        DimensionGroupSpec.emptySpec(collection).getReverseMapping();
    Multimap<String, String> expandedDimensionValues =
        ThirdEyeRequestUtils.expandDimensionGroups(dimensionValuesMap, dimensionGroups);

    ThirdEyeRequest baselineReq = new ThirdEyeRequestBuilder().setCollection(collection)
        .setMetricFunction(metricFunction).setStartTimeInclusive(baselineStart).setEndTime(baselineEnd)
        .setDimensionValues(expandedDimensionValues).build();
    ThirdEyeRequest currentReq = new ThirdEyeRequestBuilder().setCollection(collection)
        .setMetricFunction(metricFunction).setStartTimeInclusive(currentStart).setEndTime(currentEnd)
        .setDimensionValues(expandedDimensionValues).build();

    LOG.info("funnel queries for collection : {}, with name : {} ", collection, spec.getName());
    LOG.info("Generated request for funnel baseline: {}", baselineReq);
    LOG.info("Generated request for funnel current: {}", currentReq);

    // Query
    Future<QueryResult> baselineResult = queryCache.getQueryResultAsync(baselineReq);
    Future<QueryResult> currentResult = queryCache.getQueryResultAsync(currentReq);

    // Baseline data
    Map<Long, Number[]> baselineData =
        CustomDashboardResource.extractFunnelData(baselineResult.get());
    Map<Long, Number[]> currentData =
        CustomDashboardResource.extractFunnelData(currentResult.get());

    long baselineOffsetMillis = currentStart.getMillis() - baselineStart.getMillis();

    // Compose result
    List<MetricDataRow> table = ViewUtils.extractMetricDataRows(baselineData, currentData,
        currentStart.getMillis(), intraPeriodMillis, baselineOffsetMillis);

    // Get mapping of metric name to index
    Map<String, Integer> metricNameToIndex = new HashMap<>();
    List<String> resultMetrics = baselineResult.get().getMetrics();
    for (int i = 0; i < resultMetrics.size(); i++) {
      metricNameToIndex.put(resultMetrics.get(i), i);
    }

    // Filter (since query result set will contain primitive metrics for each derived one)
    List<MetricDataRow> filteredTable = new ArrayList<>();
    int metricCount = spec.getActualMetricNames().size();

    for (MetricDataRow row : table) {
      Number[] filteredBaseline = new Number[metricCount];
      Number[] filteredCurrent = new Number[metricCount];
      for (int i = 0; i < metricCount; i++) {
        String metricName = spec.getActualMetricNames().get(i);
        Integer metricIdx = metricNameToIndex.get(metricName);

        Number baselineValue = null;
        if (row.getBaseline() != null) {
          try {
            baselineValue = row.getBaseline()[metricIdx];
          } catch (Exception e) {
            LOG.error("", e);
          }
        }
        filteredBaseline[i] = baselineValue;

        Number currentValue = null;
        if (row.getCurrent() != null) {
          try {
            currentValue = row.getCurrent()[metricIdx];
          } catch (Exception e) {
            LOG.error("", e);
          }
        }
        filteredCurrent[i] = currentValue;
      }

      MetricDataRow filteredRow = new MetricDataRow(row.getBaselineTime(), filteredBaseline,
          row.getCurrentTime(), filteredCurrent);
      filteredTable.add(filteredRow);
    }

    // dates rendered on view still assume UTC return times.
    return new FunnelTable(spec, new MetricTable(filteredTable, metricCount), currentStart,
        baselineStart);
  }

  // TODO : {dpatel : move this to config cache later, would have started with it but found that out
  // late}
  void loadConfigs() throws Exception {

    funnelSpecsMap.clear();
    // looping through all the dirs, finding one funnels file and loading it up
    Set<String> collections = new HashSet<>(dataCache.getCollections());
    ObjectMapper ymlReader = new ObjectMapper(new YAMLFactory());
    if (funnelsRoot.exists()) {
      for (File f : funnelsRoot.listFiles()) {
        File funnelsFile = new File(f, FUNNELS_CONFIG_FILE_NAME);
        if (!funnelsFile.exists()) {
          LOG.warn("did not find funnels config file {} , in folder {}", FUNNELS_CONFIG_FILE_NAME,
              f.getName());
          continue;
        }

        try {
          CustomFunnelSpec spec = ymlReader.readValue(funnelsFile, CustomFunnelSpec.class);
          // add default funnel spec
          String collection = spec.getCollection();
          if (!collections.contains(collection)) {
            LOG.error("Invalid funnel collection: {}", collection);
            continue;
          }
          FunnelSpec defaultFunnelSpec = createDefaultFunnelSpec(collection);
          spec.getFunnels().put(defaultFunnelSpec.getName(), defaultFunnelSpec);
          this.funnelSpecsMap.put(collection, spec);
        } catch (Exception e) {
          LOG.error("error loading the configFile", e);
        }
      }
    }

    // add default for any collections that don't have an existing funnel spec file
    for (String collection : getCollections()) {
      if (this.funnelSpecsMap.containsKey(collection)) {
        continue;
      }
      LOG.info("Creating default funnel config for collection: {}", collection);
      CustomFunnelSpec spec = new CustomFunnelSpec();
      spec.setCollection(collection);
      spec.setFunnels(Arrays.asList(createDefaultFunnelSpec(collection)));
      this.funnelSpecsMap.put(collection, spec);
    }

  }

  /**
   * Creates a default funnel spec consisting of all the collection metrics. If the collection
   * doesn't exist, returns null.
   * @param collection
   * @throws Exception
   */
  private FunnelSpec createDefaultFunnelSpec(String collection) throws Exception {
    CollectionSchema collectionSchema = getCollectionSchema(collection);
    if (collectionSchema == null) {
      return null;
    }
    FunnelSpec defaultSpec = new FunnelSpec();
    defaultSpec.setName(DEFAULT_FUNNEL_NAME);
    defaultSpec.setVisulizationType(DEFAULT_FUNNEL_VISUALIZATION_TYPE);
    List<String> metricAliases = collectionSchema.getMetricAliases();
    List<String> metrics = collectionSchema.getMetrics();
    LinkedList<String> defaultFunnelMetrics = new LinkedList<>();
    for (int i = 0; i < metricAliases.size(); i++) {
      String metricAlias = metricAliases.get(i);
      String metric = metrics.get(i);
      metricAlias = metricAlias == null ? metric : metricAlias;
      String defaultFunnelMetric = String.format("%s=%s", metricAlias, metric);

      defaultFunnelMetrics.add(defaultFunnelMetric);
    }
    defaultSpec.setMetrics(defaultFunnelMetrics);
    return defaultSpec;
  }

  /* Returns empty list if server request fails. */
  private List<String> getCollections() {
    List<String> collections;
    try {
      collections = dataCache.getCollections();
    } catch (Exception e) {
      LOG.error("Unable to retrieve collections", e);
      collections = Collections.emptyList();
    }
    return collections;
  }

  /* Returns null if server request fails. */
  private CollectionSchema getCollectionSchema(String collection) {
    CollectionSchema result;
    try {
      result = dataCache.getCollectionSchema(collection);
    } catch (Exception e) {
      LOG.error("Unable to retrieve collection schema", e);
      result = null;
    }
    return result;
  }
}
