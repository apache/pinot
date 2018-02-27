package com.linkedin.thirdeye.dashboard.resources.v2;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dashboard.views.heatmap.HeatMapViewHandler;
import com.linkedin.thirdeye.dashboard.views.heatmap.HeatMapViewRequest;
import com.linkedin.thirdeye.dashboard.views.heatmap.HeatMapViewResponse;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.MetricExpression;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.cache.QueryCache;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
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
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();

  private final MetricConfigManager metricConfigDAO;
  private final DatasetConfigManager datasetConfigDAO;
  private final AnomalyFunctionManager anomalyFunctionDAO;
  private final AlertConfigManager alertConfigDAO;

  private final LoadingCache<String, Long> collectionMaxDataTimeCache;
  private final LoadingCache<String, String> dimensionsFilterCache;

  private final QueryCache queryCache;

  public DataResource(AnomalyFunctionFactory anomalyFunctionFactory, AlertFilterFactory alertFilterFactory) {
    this.metricConfigDAO = DAO_REGISTRY.getMetricConfigDAO();
    this.datasetConfigDAO = DAO_REGISTRY.getDatasetConfigDAO();
    this.anomalyFunctionDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
    this.alertConfigDAO = DAO_REGISTRY.getAlertConfigDAO();

    this.queryCache = CACHE_REGISTRY_INSTANCE.getQueryCache();
    this.collectionMaxDataTimeCache = CACHE_REGISTRY_INSTANCE.getDatasetMaxDataTimeCache();
    this.dimensionsFilterCache = CACHE_REGISTRY_INSTANCE.getDimensionFiltersCache();
  }

  @GET
  @Path("datasets")
  public List<String> getDatasets() {
    List<String> datasets = new ArrayList<>();
    List<DatasetConfigDTO> datasetConfigDTOs = datasetConfigDAO.findAll();
    for (DatasetConfigDTO datasetConfig : datasetConfigDTOs) {
      datasets.add(datasetConfig.getDataset());
    }
    Collections.sort(datasets);
    return datasets;
  }

//------------- endpoints to metric config -------------
  @GET
  @Path("data/metricId")
  public List<MetricConfigDTO> getMetricsByName(@QueryParam("name") String name) {
    List<MetricConfigDTO> metricConfigDTOs = metricConfigDAO.findByMetricName(name);
    return metricConfigDTOs;
  }

  @GET
  @Path("metric/{metricId}")
  public MetricConfigDTO getMetricById(@PathParam("metricId") long metricId) {
    return metricConfigDAO.findById(metricId);
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
  @Path("maxDataTime/metricId/{metricId}")
  public Long getMetricMaxDataTime(@PathParam("metricId") Long metricId) {
    MetricConfigDTO metricConfig = DAO_REGISTRY.getMetricConfigDAO().findById(metricId);
    String dataset = metricConfig.getDataset();

    long maxDataTime = Utils.getMaxDataTimeForDataset(dataset);
    return maxDataTime;
  }

  //------------- endpoint for autocomplete ----------
  @GET
  @Path("autocomplete/anomalies")
  public List<? extends Object> getWhereNameLike(@QueryParam("mode") String mode, @QueryParam("name") String name){
    if("metric".equalsIgnoreCase(mode)){
      return getMetricsWhereNameLike(name);
    }
    return Collections.emptyList();
  }


  @GET
  @Path("autocomplete/metric")
  public List<MetricConfigDTO> getMetricsWhereNameLike(@QueryParam("name") String name) {
    List<MetricConfigDTO> metricConfigs = Collections.emptyList();
    if (StringUtils.isNotBlank(name)) {
      Set<String> aliasParts = new HashSet<>(Arrays.asList(name.split("\\s+")));
      metricConfigs = metricConfigDAO.findWhereAliasLikeAndActive(aliasParts);
    }
    Collections.sort(metricConfigs, new Comparator<MetricConfigDTO>() {
      @Override
      public int compare(MetricConfigDTO o1, MetricConfigDTO o2) {
        return o1.getAlias().compareTo(o2.getAlias());
      }
    });
    return metricConfigs;
  }

  /**
   * Returns list of Anomaly functions matching given name
   * @param name
   * @return
   */
  @GET
  @Path("autocomplete/functionByName")
  public List<AnomalyFunctionDTO> getFunctionsWhereNameLike(@QueryParam("name") String name) {
    List<AnomalyFunctionDTO> functions = Collections.emptyList();
    if (StringUtils.isNotBlank(name)) {
      functions = anomalyFunctionDAO.findWhereNameLike("%" + name + "%");
    }
    return functions;
  }


  /**
   * Returns list of AnomalyFunction object matching given AlertConfigName
   * @param alertName
   * @return
   */
  @GET
  @Path("autocomplete/functionByAlertName")
  public List<AnomalyFunctionDTO> getAlertsWhereAlertNameLike(@QueryParam("alertName") String alertName) {
    List<AlertConfigDTO> alerts = Collections.emptyList();
    if (StringUtils.isNotBlank(alertName)) {
      alerts = alertConfigDAO.findWhereNameLike("%" + alertName + "%");
    }
    return getFunctionsFromAlertConfigs(alerts);
  }

  /**
   * Returns list of AnomalyFunction object matching given appName
   * @param appname
   * @return
   */
  @GET
  @Path("autocomplete/functionByAppname")
  public List<AnomalyFunctionDTO> getAlertsWhereAppNameLike(@QueryParam("appname") String appname) {
    List<AlertConfigDTO> alerts = Collections.emptyList();
    if (StringUtils.isNotBlank(appname)) {
      alerts = alertConfigDAO.findWhereApplicationLike("%" + appname + "%");
    }
   return getFunctionsFromAlertConfigs(alerts);
  }

  private List<AnomalyFunctionDTO> getFunctionsFromAlertConfigs(List<AlertConfigDTO> alerts) {
    Set<AnomalyFunctionDTO> functionsSet = new HashSet<>();
    List<AnomalyFunctionDTO> functions = new ArrayList<>();

    for (AlertConfigDTO alertConfigDTO : alerts) {
      if(alertConfigDTO.getEmailConfig() != null) {
        List<Long> functionIds = alertConfigDTO.getEmailConfig().getFunctionIds();
        for (Long functionId : functionIds) {
          AnomalyFunctionDTO anomalyFunctionDTO = anomalyFunctionDAO.findById(functionId);
          if (anomalyFunctionDTO != null) {
            functionsSet.add(anomalyFunctionDTO);
          }
        }
      }
    }
    functions.addAll(functionsSet);
    return functions;
  }


  /**
   * Returns list of AlertConfig object matching given name
   * @param name
   * @return
   */
  @GET
  @Path("autocomplete/alert")
  public List<AlertConfigDTO> getAlertsWhereNameLike(@QueryParam("name") String name) {
    List<AlertConfigDTO> alerts = Collections.emptyList();
    if (StringUtils.isNotBlank(name)) {
      alerts = alertConfigDAO.findWhereNameLike("%" + name + "%");
    }
    return alerts;
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

  /**
   * Returns a list of all possible aggregations that we will support on the front end
   * For minute level datasets, we will also support HOURS and DAYS
   * For hour level datasets, we will also support DAYS
   * For day level datasets, we will only support DAYS
   * @param metricId
   * @return list of allowed data aggregations
   */
  @GET
  @Path("agg/granularity/metric/{metricId}")
  public List<String> getDataAggregationGranularities(@PathParam("metricId") Long metricId) {

    MetricConfigDTO metricConfig = metricConfigDAO.findById(metricId);
    DatasetConfigDTO datasetConfig = ThirdEyeUtils.getDatasetConfigFromName(metricConfig.getDataset());
    int dataTimeSize = datasetConfig.bucketTimeGranularity().getSize();
    TimeUnit dataTimeUnit = datasetConfig.bucketTimeGranularity().getUnit();


    List<String> dataGranularities = new ArrayList<>();
    if (datasetConfig.isAdditive()) { // Add additional aggregation granularities only for additive datasets
      switch (dataTimeUnit) {
        case MILLISECONDS:
        case SECONDS:
        case MINUTES:
          dataGranularities.add(getDataGranularityString(dataTimeSize, TimeUnit.MINUTES));
        case HOURS:
          dataGranularities.add(getDataGranularityString(1, TimeUnit.HOURS));
        case DAYS:
        default:
          dataGranularities.add(getDataGranularityString(1, TimeUnit.DAYS));
          break;
      }
    } else { // for non additive, keep only original granularity
      dataGranularities.add(getDataGranularityString(dataTimeSize, dataTimeUnit));
    }
    return dataGranularities;
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
    List<MetricExpression> metricExpressions = Utils.convertToMetricExpressions(metric,
        metricConfigDTO.getDefaultAggFunction(), collection);

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
    response.setInverseMetric(metricConfigDTO.isInverseMetric());

    return response;
  }


  @GET
  @Path("anomalies/ranges")
  public Map<Long, List<TimeRange>> getAnomalyTimeRangesByMetricIds(
      @QueryParam("metricIds") String metricIds,
      @QueryParam("start") Long start,
      @QueryParam("end") Long end,
      @QueryParam("filters") String filters) {

    if (metricIds == null)
      throw new IllegalArgumentException("Must provide metricIds");

    if (start == null)
      throw new IllegalArgumentException("Must provide start timestamp");

    if (end == null)
      throw new IllegalArgumentException("Must provide end timestamp");

    List<Long> ids = new ArrayList<>();
    for (String metricId : metricIds.split(",")) {
      ids.add(Long.parseLong(metricId));
    }

    // fetch anomalies in time range
    Map<Long, List<MergedAnomalyResultDTO>> anomalies = DAO_REGISTRY.getMergedAnomalyResultDAO().findAnomaliesByMetricIdsAndTimeRange(ids, start, end);
    int countAll = countNested(anomalies);

    // apply search filters
    if (filters != null && !filters.isEmpty()) {
      Multimap<String, String> filterMap = ThirdEyeUtils.convertToMultiMap(filters);

      for (Map.Entry<Long, List<MergedAnomalyResultDTO>> entry : anomalies.entrySet()) {
        entry.setValue(applyAnomalyFilters(entry.getValue(), filterMap));
      }

      int countPassed = countNested(anomalies);
      LOG.info("Fetched {} anomalies ({} after filter) for time range {}-{}", countAll, countPassed, start, end);

    } else {
      // no filter
      LOG.info("Fetched {} anomalies for time range {}-{}", countAll, start, end);
    }

    // extract and truncate time ranges
    Map<Long, List<TimeRange>> output = new HashMap<>();
    for(Map.Entry<Long, List<MergedAnomalyResultDTO>> entry : anomalies.entrySet()) {
      output.put(entry.getKey(), truncateRanges(extractAnomalyTimeRanges(entry.getValue()), start, end));
    }

    return output;
  }

  /**
   * Returns a list of TimeRanges that correspond to anomalous time windows covered by at least
   * one anomaly. If multiple anomalies overlap or form adjacent time windows, they're merged
   * into a single range.
   *
   * @param anomalies merged anomalies
   * @return list of time ranges
   */
  static List<TimeRange> extractAnomalyTimeRanges(List<MergedAnomalyResultDTO> anomalies) {
    if(anomalies.isEmpty()) {
      return Collections.emptyList();
    }

    List<MergedAnomalyResultDTO> sorted = new ArrayList<>(anomalies);
    Collections.sort(sorted, new Comparator<MergedAnomalyResultDTO>() {
      @Override
      public int compare(MergedAnomalyResultDTO o1, MergedAnomalyResultDTO o2) {
        return Long.compare(o1.getStartTime(), o2.getStartTime());
      }
    });

    List<TimeRange> ranges = new ArrayList<>();
    Iterator<MergedAnomalyResultDTO> itAnomaly = sorted.iterator();
    MergedAnomalyResultDTO first = itAnomaly.next();
    long currStart = first.getStartTime();
    long currEnd = first.getEndTime();

    while(itAnomaly.hasNext()) {
      MergedAnomalyResultDTO anomaly = itAnomaly.next();
      if (currEnd >= anomaly.getStartTime()) {
        currEnd = Math.max(currEnd, anomaly.getEndTime());
      } else {
        ranges.add(new TimeRange(currStart, currEnd));
        currStart = anomaly.getStartTime();
        currEnd = anomaly.getEndTime();
      }
    }

    ranges.add(new TimeRange(currStart, currEnd));

    return ranges;
  }

  /**
   * Returns a list of TimeRanges truncated to a given start and end timestamp. If the input
   * TimeRange is outside the boundaries it is omitted. If it overlaps partially, it is
   * truncated and included.
   *
   * @param ranges list of time ranges
   * @param start start timestamp (inclusive)
   * @param end end timestamp (exclusive)
   * @return list of truncated time ranges
   */
  static List<TimeRange> truncateRanges(List<TimeRange> ranges, long start, long end) {
    List<TimeRange> output = new ArrayList<>();
    for (TimeRange r : ranges) {
      if (r.getStart() < end && r.getEnd() > start) {
        output.add(new TimeRange(Math.max(r.getStart(), start), Math.min(r.getEnd(), end)));
      }
    }
    return output;
  }

  /**
   * Returns a list of anomalies that fulfills the dimension filter requirements specified in
   * {@code filters}. Returns an empty list if no anomaly passes the filter. If {@code filters}
   * contains multiple values for the same dimension key, ANY match will pass the filter.
   *
   * @param anomalies list of anomalies
   * @param filters dimension filter multimap
   * @return list of filtered anomalies
   */
  static List<MergedAnomalyResultDTO> applyAnomalyFilters(List<MergedAnomalyResultDTO> anomalies, Multimap<String, String> filters) {
    List<MergedAnomalyResultDTO> output = new ArrayList<>();
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      if (applyAnomalyFilters(anomaly, filters)) {
        output.add(anomaly);
      }
    }
    return output;
  }

  /**
   * Returns {@code true} if a given anomaly passes the dimension filters {@code filters}, or
   * {@code false} otherwise. If {@code filters} contains multiple values for the same dimension
   * key, ANY match will pass the filter.
   *
   * @param anomaly anomaly to filter
   * @param filters dimension filter multimap
   * @return {@code true} if anomaly passed the filters, {@code false} otherwise
   */
  static boolean applyAnomalyFilters(MergedAnomalyResultDTO anomaly, Multimap<String, String> filters) {
    Multimap<String, String> anomalyFilter = AnomaliesResource.generateFilterSetForTimeSeriesQuery(anomaly);
    for (String filterKey : filters.keySet()) {
      if (!anomalyFilter.containsKey(filterKey))
        return false;

      Collection<String> filterValues = filters.get(filterKey);
      if (!filterValues.containsAll(anomalyFilter.get(filterKey)))
        return false;
    }
    return true;
  }

  /**
   * Returns the total count of elements in collections nested within a map.
   *
   * @param map map with nested collection
   * @return total nested item count
   */
  static int countNested(Map<?, ? extends Collection<?>> map) {
    int count = 0;
    for (Map.Entry<?, ? extends Collection<?>> entry : map.entrySet()) {
      count += entry.getValue().size();
    }
    return count;
  }


  /**
   * Generates data granularity string for dropdown in the root cause page
   * @param dataTimeSize
   * @param dataTimeUnit
   * @return data granularity string
   */
  private String getDataGranularityString(int dataTimeSize, TimeUnit dataTimeUnit) {
    String dataGranularity = null;
    if (dataTimeSize == 1) {
      dataGranularity = dataTimeUnit.toString();
    } else {
      dataGranularity = String.format("%d_%s", dataTimeSize, dataTimeUnit);
    }
    return dataGranularity;
  }
}
