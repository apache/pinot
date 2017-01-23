package com.linkedin.thirdeye.dashboard.resources.v2;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.cache.MetricDataset;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.AnomaliesSummary;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.MetricSummary;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.WowSummary;
import com.linkedin.thirdeye.dashboard.views.GenericResponse;
import com.linkedin.thirdeye.dashboard.views.heatmap.HeatMapViewHandler;
import com.linkedin.thirdeye.dashboard.views.heatmap.HeatMapViewRequest;
import com.linkedin.thirdeye.dashboard.views.heatmap.HeatMapViewResponse;
import com.linkedin.thirdeye.dashboard.views.tabular.TabularViewHandler;
import com.linkedin.thirdeye.dashboard.views.tabular.TabularViewRequest;
import com.linkedin.thirdeye.dashboard.views.tabular.TabularViewResponse;
import com.linkedin.thirdeye.datalayer.bao.DashboardConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DashboardConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

/**
 * Do's and Dont's
 * ================
 * 1. Prefer PathParams over QueryParams
 * 2. Protocols : use Post for new entity creation, Put for update, Delete for delete and Get for retrieval
 * 3. Dont use OBJECT_MAPPER unnecessarily as REST library already takes care of marshalling your object to JSON
 *
 * 4. Errors: there are few ways to handle server side errors
 *    a. catch exception and throw as WebApplicationException : its a REST library exception, you can pass your error response etc into this exception
 *    b. Add a ExceptionMapper and register it in the dw environment
 *    c. Add a web filter / intercepter to catch and convert RTEs to web exception
 */
@Path(value = "/data")
@Produces(MediaType.APPLICATION_JSON)
public class DataResource {
  private static final Logger LOG = LoggerFactory.getLogger(DataResource.class);
  private static final DAORegistry daoRegistry = DAORegistry.getInstance();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();

  private final MetricConfigManager metricConfigDAO;
  private final DatasetConfigManager datasetConfigDAO;
  private final DashboardConfigManager dashboardConfigDAO;

  private final LoadingCache<String, Long> collectionMaxDataTimeCache;
  private final LoadingCache<String, String> dimensionsFilterCache;

  private final QueryCache queryCache;
  private AnomaliesResource anomaliesResoure;

  public DataResource() {
    metricConfigDAO = daoRegistry.getMetricConfigDAO();
    datasetConfigDAO = daoRegistry.getDatasetConfigDAO();
    dashboardConfigDAO = daoRegistry.getDashboardConfigDAO();

    this.queryCache = CACHE_REGISTRY_INSTANCE.getQueryCache();
    this.collectionMaxDataTimeCache = CACHE_REGISTRY_INSTANCE.getCollectionMaxDataTimeCache();
    this.dimensionsFilterCache = CACHE_REGISTRY_INSTANCE.getDimensionFiltersCache();
    this.anomaliesResoure = new AnomaliesResource();
  }

  //------------- endpoints to fetch summary -------------
  @GET
  @Path("summary/metrics")
  public List<String> getMetricNamesForDataset(@QueryParam("dataset") String dataset) {
    List<MetricConfigDTO> metrics = new ArrayList<>();
    if (Strings.isNullOrEmpty(dataset)) {
      metrics.addAll(metricConfigDAO.findAll());
    } else {
      metrics.addAll(metricConfigDAO.findActiveByDataset(dataset));
    }
    List<String> metricsNames = new ArrayList<>();
    for (MetricConfigDTO metricConfigDTO : metrics) {
      metricsNames.add(metricConfigDTO.getName());
    }
    return metricsNames;
  }

  @GET
  @Path("summary/dashboards")
  public List<String> getDashboardNames() {
    List<String> output = new ArrayList<>();
    List<DashboardConfigDTO> dashboardConfigDTOs = dashboardConfigDAO.findAll();
    for (DashboardConfigDTO dashboardConfigDTO : dashboardConfigDTOs) {
      output.add(dashboardConfigDTO.getName());
    }
    return output;
  }

  @GET
  @Path("summary/datasets")
  public List<String> getDatasetNames() {
    List<String> output = new ArrayList<>();
    List<DatasetConfigDTO> datasetConfigDTOs = datasetConfigDAO.findAll();
    for (DatasetConfigDTO dto : datasetConfigDTOs) {
      output.add(dto.getDataset());
    }
    return output;
  }

  @GET
  @Path("maxtime/{dataset}")
  public Map<String, Long> getMetricMaxDataTime(@PathParam("dataset") String dataset) {
    return null;
  }

  //------------- endpoint for autocomplete ----------
  @GET
  @Path("autocomplete/anomalies")
  public List<? extends Object> getWhereNameLike(@QueryParam("mode") String mode, @QueryParam("name") String name){
    if("metric".equalsIgnoreCase(mode)){
      return getMetricsWhereNameLike(name);
    }
    if("dashboard".equalsIgnoreCase(mode)){
      return getDashboardsWhereNameLike(name);
    }
    return Collections.emptyList();
  }
  @GET
  @Path("autocomplete/dashboard")
  public List<DashboardConfigDTO> getDashboardsWhereNameLike(@QueryParam("name") String name) {
    return dashboardConfigDAO.findWhereNameLike("%" + name + "%");
  }

  @GET
  @Path("autocomplete/metric")
  public List<MetricConfigDTO> getMetricsWhereNameLike(@QueryParam("name") String name) {
    return metricConfigDAO.findWhereNameLike("%" + name + "%");
  }

  @GET
  @Path("autocomplete/dimensions/metric/{metricId}")
  public List<String> getDimensionsForMetric(@PathParam("metricId") Long metricId) {
    List<String> list = new ArrayList<>();
    list.add("All");
    try {
      MetricConfigDTO metricConfigDTO = metricConfigDAO.findById(metricId);
      DatasetConfigDTO datasetConfigDTO = datasetConfigDAO.findByDataset(metricConfigDTO.getDataset());
      list.addAll(datasetConfigDTO.getDimensions());
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
    return list;
  }

  @GET
  @Path("autocomplete/filters/metric/{metricId}")
  public Map<String, List<String>> getFiltersForMetric(@PathParam("metricId") Long metricId) {
    Map<String, List<String>> filterMap = new HashMap<>();
    try {
    // TODO : cache this
    MetricConfigDTO metricConfigDTO = metricConfigDAO.findById(metricId);
    DatasetConfigDTO datasetConfigDTO = datasetConfigDAO.findByDataset(metricConfigDTO.getDataset());
    String dimensionFiltersJson = dimensionsFilterCache.get(datasetConfigDTO.getDataset());
      if (!Strings.isNullOrEmpty(dimensionFiltersJson)) {
        filterMap = OBJECT_MAPPER.readValue(dimensionFiltersJson, LinkedHashMap.class);
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new WebApplicationException(e);
    }
    return filterMap;
  }

  @GET
  @Path("agg/granularity/metric/{metricId}")
  public List<String> getDataAggregationGranularity(@PathParam("metricId") Long metricId) {
    List<String> list = new ArrayList<>();
    list.add("DAYS");
    MetricConfigDTO metricConfigDTO = metricConfigDAO.findById(metricId);
    DatasetConfigDTO datasetConfigDTO = datasetConfigDAO.findByDataset(metricConfigDTO.getDataset());
    int dataAggSize = datasetConfigDTO.getTimeDuration();
    String dataGranularity = datasetConfigDTO.getTimeUnit().name();
    if (dataGranularity.equals("DAYS")) {
      // do nothing
    } else {
      list.add("HOURS");
      if (dataGranularity.equals("MINUTES")) {
        if (dataAggSize == 1) {
          list.add("MINUTES");
        } else {
          list.add(dataAggSize+ "_MINUTES");
        }
      }
    }
    return list;
  }

  //------------- HeatMap -----------------
  @GET
  @Path(value = "heatmap/{metricId}/{currentStart}/{currentEnd}/{baselineStart}/{baselineEnd}")
  @Produces(MediaType.APPLICATION_JSON)
  public HeatMapViewResponse getHeatMap(
      @QueryParam("filters") String filters,
      @PathParam("baselineStart") Long baselineStart, @PathParam("baselineEnd") Long baselineEnd,
      @PathParam("currentStart") Long currentStart, @PathParam("currentEnd") Long currentEnd,
      @PathParam("metricId") Long metricId)
      throws Exception {
    MetricConfigDTO metricConfigDTO = metricConfigDAO.findById(metricId);

    String collection = metricConfigDTO.getDataset();
    String metric = metricConfigDTO.getName();

    HeatMapViewRequest request = new HeatMapViewRequest();
    request.setCollection(collection);
    List<MetricExpression> metricExpressions = Utils.convertToMetricExpressions(metric, MetricAggFunction.SUM, collection);
    request.setMetricExpressions(metricExpressions);
    long maxDataTime = collectionMaxDataTimeCache.get(collection);
    if (currentEnd > maxDataTime) {
      long delta = currentEnd - maxDataTime;
      currentEnd = currentEnd - delta;
      baselineEnd = baselineEnd - delta;
    }

    // See {@link #getDashboardData} for the reason that the start and end time are stored in a
    // DateTime object with data's timezone.
    DateTimeZone timeZoneForCollection = Utils.getDataTimeZone(collection);
    request.setBaselineStart(new DateTime(baselineStart, timeZoneForCollection));
    request.setBaselineEnd(new DateTime(baselineEnd, timeZoneForCollection));
    request.setCurrentStart(new DateTime(currentStart, timeZoneForCollection));
    request.setCurrentEnd(new DateTime(currentEnd, timeZoneForCollection));

    // filter
    if (filters != null && !filters.isEmpty()) {
      filters = URLDecoder.decode(filters, "UTF-8");
      request.setFilters(ThirdEyeUtils.convertToMultiMap(filters));
    }

    HeatMapViewHandler handler = new HeatMapViewHandler(queryCache);
    HeatMapViewResponse response = handler.process(request);

    return response;
  }



  //----------------- dashboard end points -------------
  @GET
  @Path("dashboard/metricids")
  public List<Long> getMetricIdsByDashboard(@QueryParam("name") String name) {
    if (StringUtils.isBlank(name)) {
      return Collections.emptyList();
    }
    DashboardConfigDTO dashboard = dashboardConfigDAO.findByName(name);
    return dashboard.getMetricIds();
  }

  /**
   * Returns percentage change between current values and baseline values. The values are
   * aggregated according to the number of buckets. If the bucket number is 1, then all values
   * between the given time ranges are sorted to the corresponding bucket and aggregated.
   *
   * Note: For current implementation, we assume the number of buckets is always 1.
   */
  @GET
  @Path("dashboard/metricsummary")
  public List<MetricSummary> getMetricSummary(@QueryParam("dashboard") String dashboard,
      @QueryParam("timeRange") String timeRange) {
    List<MetricSummary> metricsSummary = new ArrayList<>();

    if (StringUtils.isBlank(dashboard)) {
      return metricsSummary;
    }

    List<Long> metricIds = getMetricIdsByDashboard(dashboard);

    // Sort metric's id and metric expression by collections
    Multimap<String, Long> datasetToMetrics = ArrayListMultimap.create();
    Multimap<String, MetricExpression> datasetToMetricExpressions = ArrayListMultimap.create();
    Map<Long, MetricConfigDTO> metricIdToMetricConfig = new HashMap<>();
    for (long metricId : metricIds) {
      MetricConfigDTO metricConfig = metricConfigDAO.findById(metricId);
      metricIdToMetricConfig.put(metricId, metricConfig);
      datasetToMetrics.put(metricConfig.getDataset(), metricId);
      datasetToMetricExpressions.put(metricConfig.getDataset(), ThirdEyeUtils.getMetricExpressionFromMetricConfig(metricConfig));
    }

    // Create query request for each collection
    for (String dataset : datasetToMetrics.keySet()) {
      TabularViewRequest request = new TabularViewRequest();
      request.setCollection(dataset);
      request.setMetricExpressions(new ArrayList<>(datasetToMetricExpressions.get(dataset)));

      // The input start and end time (i.e., currentStart, currentEnd, baselineStart, and
      // baselineEnd) are given in millisecond since epoch, which is timezone insensitive. On the
      // other hand, the start and end time of the request to be sent to backend database (e.g.,
      // Pinot) could be converted to SimpleDateFormat, which is timezone sensitive. Therefore,
      // we need to store user's start and end time in DateTime objects with data's timezone
      // in order to ensure that the conversion to SimpleDateFormat is always correct regardless
      // user and server's timezone, including daylight saving time.
      String[] tokens = timeRange.split("_");
      TimeGranularity timeGranularity = new TimeGranularity(Integer.valueOf(tokens[0]), TimeUnit.valueOf(tokens[1]));
      long currentEnd = Utils.getMaxDataTimeForDataset(dataset);
      long currentStart = currentEnd - TimeUnit.MILLISECONDS.convert(Long.valueOf(tokens[0]), TimeUnit.valueOf(tokens[1]));

      DateTimeZone timeZoneForCollection = Utils.getDataTimeZone(dataset);
      request.setBaselineStart(new DateTime(currentStart, timeZoneForCollection).minusDays(7));
      request.setBaselineEnd(new DateTime(currentEnd, timeZoneForCollection).minusDays(7));
      request.setCurrentStart(new DateTime(currentStart, timeZoneForCollection));
      request.setCurrentEnd(new DateTime(currentEnd, timeZoneForCollection));
      request.setTimeGranularity(timeGranularity);

      TabularViewHandler handler = new TabularViewHandler(queryCache);
      try {
        TabularViewResponse tabularViewResponse = handler.process(request);
        for (String metric : tabularViewResponse.getMetrics()) {
          MetricDataset metricDataset = new MetricDataset(metric, dataset);
          MetricConfigDTO metricConfig = CACHE_REGISTRY_INSTANCE.getMetricConfigCache().get(metricDataset);
          Long metricId = metricConfig.getId();
          GenericResponse response = tabularViewResponse.getData().get(metric);

          MetricSummary metricSummary = new MetricSummary();
          metricSummary.setMetricId(metricId);
          metricSummary.setMetricName(metricConfig.getName());
          metricSummary.setMetricAlias(metricConfig.getAlias());
          String[] responseData = response.getResponseData().get(0);
          double baselineValue = Double.valueOf(responseData[0]);
          double curentvalue = Double.valueOf(responseData[1]);
          double percentageChange = (curentvalue - baselineValue) * 100 / baselineValue;
          metricSummary.setBaselineValue(baselineValue);
          metricSummary.setCurrentValue(curentvalue);
          metricSummary.setWowPercentageChange(percentageChange);
          AnomaliesSummary anomaliesSummary = anomaliesResoure.getAnomalyCountForMetricInRange(metricId, currentStart, currentEnd);
          metricSummary.setAnomaliesSummary(anomaliesSummary);

          metricsSummary.add(metricSummary);

        }
      } catch (Exception e) {
        LOG.error("Exception while processing /data/tabular call", e);
      }
    }


    return metricsSummary;
  }



  @GET
  @Path("dashboard/anomalysummary")
  public Map<String, List<AnomaliesSummary>> getAnomalySummary(
      @QueryParam("dashboard") String dashboard,
      @QueryParam("timeRanges") String timeRanges) {
    List<Long> metricIds = getMetricIdsByDashboard(dashboard);
    List<String> timeRangesList = Lists.newArrayList(timeRanges.split(","));
    Map<String, Long> timeRangeToDurationMap = new HashMap<>();
    for (String timeRange : timeRangesList) {
      String[] tokens = timeRange.split("_");
      long duration = TimeUnit.MILLISECONDS.convert(Long.valueOf(tokens[0]), TimeUnit.valueOf(tokens[1]));
      timeRangeToDurationMap.put(timeRange, duration);
    }

    Map<String, List<AnomaliesSummary>> metricAliasToAnomaliesSummariesMap = new HashMap<>();
    for (Long metricId : metricIds) {
      List<AnomaliesSummary> summaries = new ArrayList<>();

      MetricConfigDTO metricConfig = metricConfigDAO.findById(metricId);
      String metricAlias = metricConfig.getAlias();
      String dataset = metricConfig.getDataset();

      long endTime = Utils.getMaxDataTimeForDataset(dataset);

      for (String timeRange : timeRangesList) {
        long startTime = endTime - timeRangeToDurationMap.get(timeRange);
        AnomaliesSummary summary = anomaliesResoure.getAnomalyCountForMetricInRange(metricId, startTime, endTime);
        summaries.add(summary);
      }
      metricAliasToAnomaliesSummariesMap.put(metricAlias, summaries);
    }
    return metricAliasToAnomaliesSummariesMap;
  }

  @GET
  @Path("dashboard/wowsummary")
  public WowSummary getWowSummary(
      @QueryParam("dashboard") String dashboard,
      @QueryParam("timeRanges") String timeRanges) {
    WowSummary wowSummary = new WowSummary();

    if (StringUtils.isBlank(dashboard)) {
      return wowSummary;
    }

    List<Long> metricIds = getMetricIdsByDashboard(dashboard);
    List<String> timeRangeLabels = Lists.newArrayList(timeRanges.split(","));

    // Sort metric's id and metric expression by collections
    Multimap<String, Long> datasetToMetrics = ArrayListMultimap.create();
    Multimap<String, MetricExpression> datasetToMetricExpressions = ArrayListMultimap.create();
    Map<Long, MetricConfigDTO> metricIdToMetricConfig = new HashMap<>();
    for (long metricId : metricIds) {
      MetricConfigDTO metricConfig = metricConfigDAO.findById(metricId);
      metricIdToMetricConfig.put(metricId, metricConfig);
      datasetToMetrics.put(metricConfig.getDataset(), metricId);
      datasetToMetricExpressions.put(metricConfig.getDataset(), ThirdEyeUtils.getMetricExpressionFromMetricConfig(metricConfig));
    }

    Multimap<String, MetricSummary> metricAliasToMetricSummariesMap = ArrayListMultimap.create();
    // Create query request for each collection
    for (String dataset : datasetToMetrics.keySet()) {
      TabularViewRequest request = new TabularViewRequest();
      request.setCollection(dataset);
      request.setMetricExpressions(new ArrayList<>(datasetToMetricExpressions.get(dataset)));

      // The input start and end time (i.e., currentStart, currentEnd, baselineStart, and
      // baselineEnd) are given in millisecond since epoch, which is timezone insensitive. On the
      // other hand, the start and end time of the request to be sent to backend database (e.g.,
      // Pinot) could be converted to SimpleDateFormat, which is timezone sensitive. Therefore,
      // we need to store user's start and end time in DateTime objects with data's timezone
      // in order to ensure that the conversion to SimpleDateFormat is always correct regardless
      // user and server's timezone, including daylight saving time.
      for (String timeRangeLabel :  timeRangeLabels) {

        DateTimeZone timeZoneForCollection = Utils.getDataTimeZone(dataset);
        TimeRange timeRange = getTimeRangeFromLabel(dataset, timeZoneForCollection, timeRangeLabel);
        long currentEnd = timeRange.getEnd();
        long currentStart = timeRange.getStart();
        System.out.println(timeRangeLabel + "Current start end " + new DateTime(currentStart) + " " + new DateTime(currentEnd));
        TimeGranularity timeGranularity = new TimeGranularity(1, TimeUnit.HOURS);
        request.setBaselineStart(new DateTime(currentStart, timeZoneForCollection).minusDays(7));
        request.setBaselineEnd(new DateTime(currentEnd, timeZoneForCollection).minusDays(7));
        request.setCurrentStart(new DateTime(currentStart, timeZoneForCollection));
        request.setCurrentEnd(new DateTime(currentEnd, timeZoneForCollection));
        request.setTimeGranularity(timeGranularity);

        TabularViewHandler handler = new TabularViewHandler(queryCache);
        try {
          TabularViewResponse tabularViewResponse = handler.process(request);
          for (String metric : tabularViewResponse.getMetrics()) {
            MetricDataset metricDataset = new MetricDataset(metric, dataset);
            MetricConfigDTO metricConfig = CACHE_REGISTRY_INSTANCE.getMetricConfigCache().get(metricDataset);
            Long metricId = metricConfig.getId();
            String metricAlias = metricConfig.getAlias();
            GenericResponse response = tabularViewResponse.getData().get(metric);

            MetricSummary metricSummary = new MetricSummary();
            metricSummary.setMetricId(metricId);
            metricSummary.setMetricName(metricConfig.getName());
            metricSummary.setMetricAlias(metricAlias);

            List<String[]> data = response.getResponseData();
            double baselineValue = 0;
            double currentValue = 0;
            for (String[] responseData : data) {
              baselineValue = baselineValue + Double.valueOf(responseData[0]);
              currentValue = currentValue + Double.valueOf(responseData[1]);
            }
            double percentageChange = (currentValue - baselineValue) * 100 / baselineValue;
            metricSummary.setBaselineValue(baselineValue);
            metricSummary.setCurrentValue(currentValue);
            metricSummary.setWowPercentageChange(percentageChange);
            metricAliasToMetricSummariesMap.put(metricAlias, metricSummary);
          }
        } catch (Exception e) {
          LOG.error("Exception while processing /data/tabular call", e);
        }
      }
    }
    wowSummary.setMetricAliasToMetricSummariesMap(metricAliasToMetricSummariesMap);
    return wowSummary;
  }


  /**
   *  convert label from WowSummaryModel to a TimeRange
   * @param dataset
   * @param timeZoneForCollection
   * @param label
   * @return
   */
  private TimeRange getTimeRangeFromLabel(String dataset, DateTimeZone timeZoneForCollection, String label) {
    long start = 0;
    long end = 0;
    long datasetMaxTime = Utils.getMaxDataTimeForDataset(dataset);
    switch (label) {
      case "Most Recent Hour":
        end = datasetMaxTime;
        start = end - TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
        break;
      case "Today":
        end = System.currentTimeMillis();
        start = new DateTime().withTimeAtStartOfDay().getMillis();
        break;
      case "Yesterday":
        end = new DateTime().withTimeAtStartOfDay().getMillis();
        start = end - TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
        break;
      case "Last 7 Days":
        end = System.currentTimeMillis();
        start = new DateTime(end).minusDays(6).withTimeAtStartOfDay().getMillis();
        break;
      default:
    }
    TimeRange timeRange = new TimeRange(start, end);
    return timeRange;
  }


}
