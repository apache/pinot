package com.linkedin.thirdeye.dashboard.resources.v2;

import com.google.common.base.Strings;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dashboard.configs.DashboardConfig;

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.thirdeye.client.ResponseParserUtils.CACHE_REGISTRY_INSTANCE;

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

  private final MetricConfigManager metricConfigDAO;
  private final DatasetConfigManager datasetConfigDAO;
  private final DashboardConfigManager dashboardConfigDAO;

  private final LoadingCache<String, Long> collectionMaxDataTimeCache;

  private final QueryCache queryCache;

  public DataResource() {
    metricConfigDAO = daoRegistry.getMetricConfigDAO();
    datasetConfigDAO = daoRegistry.getDatasetConfigDAO();
    dashboardConfigDAO = daoRegistry.getDashboardConfigDAO();

    this.queryCache = CACHE_REGISTRY_INSTANCE.getQueryCache();
    this.collectionMaxDataTimeCache = CACHE_REGISTRY_INSTANCE.getCollectionMaxDataTimeCache();
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

  //------------- endpoints to fetch config objects -------------
  // metric end points
  @GET
  @Path("metrics")
  public List<MetricConfigDTO> getMetrics(
      @QueryParam("pageId") @DefaultValue("0") int pageId,
      @QueryParam("numResults") @DefaultValue("1000") int numResults,
      @QueryParam("dataset") String dataset, @QueryParam("metric") String metric
  ) {
    // TODO: add pagination support through out the data managers
    List<MetricConfigDTO> output = new ArrayList<>();
    if (StringUtils.isEmpty(dataset)) {
      output.addAll(metricConfigDAO.findAll());
    } else {
      if (StringUtils.isNotEmpty(metric)) {
        output.addAll(metricConfigDAO.findActiveByDataset(dataset));
      } else {
        output.add(metricConfigDAO.findByMetricAndDataset(metric, dataset));
      }
    }
    return output;
  }

  @GET
  @Path("metric/{id}")
  public MetricConfigDTO getMetricById(@PathParam("id") Long id) {
    return metricConfigDAO.findById(id);
  }

  //------------- endpoint for autocomplete ----------
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

  // dataset end points
  @GET
  @Path("datasets")
  public List<DatasetConfigDTO> getDatasets(
      @QueryParam("pageId") @DefaultValue("0") int pageId,
      @QueryParam("numResults") @DefaultValue("1000") int numResults) {

    return null;
  }

  @GET
  @Path("dataset/{id}")
  public DatasetConfigDTO getDatasetById(@PathParam("id") Long id) {
    return datasetConfigDAO.findById(id);
  }

  @GET
  @Path("dataset")
  public DatasetConfigDTO getDatasetByName(@QueryParam("dataset") String dataset) {
    return datasetConfigDAO.findByDataset(dataset);
  }

  @GET
  @Path(value = "filters/{dataset}")
  public Map<String, List<String>> getFilters(@PathParam("dataset") String dataset) {
    return null;
  }

  // dashboard end points
  @GET
  @Path("dashboard")
  public List<DashboardConfig> getDashboards(@QueryParam("dashboard") String dashboard) {
    return new ArrayList<>();
  }

  @GET
  @Path("dashboard/{id}")
  public DashboardConfig getDashboardById(@PathParam("id") Long id) {
    return null;
  }

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
  @Path("dashboard/wowsummary")
  public TabularViewResponse getWoWSummary(@QueryParam("name") String dashboardName,
      @QueryParam("currentStart") Long currentStart, @QueryParam("currentEnd") Long currentEnd,
      @QueryParam("currentStart") Long baselineStart, @QueryParam("currentEnd") Long baselineEnd) {
    if (StringUtils.isBlank(dashboardName)) {
      return null;
    }

    List<Long> metricIds = getMetricIdsByDashboard(dashboardName);

    // Sort metric's id and metric expression by collections
    Multimap<String, Long> collectionToMetrics = ArrayListMultimap.create();
    Multimap<String, MetricExpression> collectionToMetricExpressions = ArrayListMultimap.create();
    for (long metricId : metricIds) {
      MetricConfigDTO metricConfig = metricConfigDAO.findById(metricId);
      collectionToMetrics.put(metricConfig.getDataset(), metricId);

      MetricExpression metricExpression = ThirdEyeUtils.getMetricExpressionFromMetricConfig(metricConfig);
      collectionToMetricExpressions.put(metricConfig.getDataset(), metricExpression);
    }

    // Get the smallest max date time among all collection
    long maxCollectionDateTime = getSmallestMaxDateTimeFromCollections(collectionToMetrics.keySet());
    if (currentEnd > maxCollectionDateTime) {
      long delta = currentEnd - maxCollectionDateTime;
      currentEnd = currentEnd - delta;
      baselineEnd = baselineEnd - delta;
    }

    // Create query request for each collection
    Map<String, TabularViewResponse> collectionToTabularViewResponse = new HashMap<>();
    for (String collection : collectionToMetrics.keySet()) {
      TabularViewRequest request = new TabularViewRequest();
      request.setCollection(collection);
      request.setMetricExpressions(new ArrayList<>(collectionToMetricExpressions.get(collection)));

      // The input start and end time (i.e., currentStart, currentEnd, baselineStart, and
      // baselineEnd) are given in millisecond since epoch, which is timezone insensitive. On the
      // other hand, the start and end time of the request to be sent to backend database (e.g.,
      // Pinot) could be converted to SimpleDateFormat, which is timezone sensitive. Therefore,
      // we need to store user's start and end time in DateTime objects with data's timezone
      // in order to ensure that the conversion to SimpleDateFormat is always correct regardless
      // user and server's timezone, including daylight saving time.
      DateTimeZone timeZoneForCollection = DateTimeZone.UTC;
      try {
        timeZoneForCollection = Utils.getDataTimeZone(collection);
      } catch (ExecutionException e) {
        LOG.info("Use UTC timezone for collection={} because failed to get its timezone.");
      }
      request.setBaselineStart(new DateTime(baselineStart, timeZoneForCollection));
      request.setBaselineEnd(new DateTime(baselineEnd, timeZoneForCollection));
      request.setCurrentStart(new DateTime(currentStart, timeZoneForCollection));
      request.setCurrentEnd(new DateTime(currentEnd, timeZoneForCollection));

      TabularViewHandler handler = new TabularViewHandler(queryCache);
      try {
        TabularViewResponse response = handler.process(request);
        collectionToTabularViewResponse.put(collection, response);
      } catch (Exception e) {
        LOG.error("Exception while processing /data/tabular call", e);
      }
    }

    // Merge responses of collections
    TabularViewResponse mergedResponse = null;
    for (String collection : collectionToMetrics.keySet()) {
      if (mergedResponse == null) {
        mergedResponse = collectionToTabularViewResponse.get(collection);
        continue;
      }
      TabularViewResponse responseToBeMerged = collectionToTabularViewResponse.get(collection);
      mergedResponse.getMetrics().addAll(responseToBeMerged.getMetrics());
      mergedResponse.getData().putAll(responseToBeMerged.getData());
      // TODO: merge response time bucket and summary
    }

    return mergedResponse;
  }

  /**
   * Get the smallest max date time of the collections
   * @param collections list of collection names
   * @return the smallest max date time among the given collections
   */
  private long getSmallestMaxDateTimeFromCollections(Collection<String> collections) {
    long maxDateTime = Long.MAX_VALUE;
    for (String collection : collections) {
      try {
        long collectionMaxDataTime = collectionMaxDataTimeCache.get(collection);
        maxDateTime = Math.min(maxDateTime, collectionMaxDataTime);
      } catch (ExecutionException e) {
        LOG.warn("Unable to get max date time of the collection: {}", collection);
      }
    }
    return maxDateTime;
  }
}
