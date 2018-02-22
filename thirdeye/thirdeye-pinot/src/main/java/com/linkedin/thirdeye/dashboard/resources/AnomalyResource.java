package com.linkedin.thirdeye.dashboard.resources;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Multimap;
import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomaly.alert.util.AlertFilterHelper;
import com.linkedin.thirdeye.anomaly.detection.AnomalyDetectionInputContext;
import com.linkedin.thirdeye.anomaly.detection.AnomalyDetectionInputContextBuilder;
import com.linkedin.thirdeye.anomaly.onboard.utils.FunctionCreationUtils;
import com.linkedin.thirdeye.anomaly.views.AnomalyTimelinesView;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyFeedback;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.constant.AnomalyResultSource;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dashboard.resources.v2.AnomaliesResource;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.AutotuneConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.AutotuneConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.pojo.MetricConfigBean;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.detector.email.filter.AlertFilter;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.email.filter.BaseAlertFilter;
import com.linkedin.thirdeye.detector.email.filter.DummyAlertFilter;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;
import com.linkedin.thirdeye.detector.metric.transfer.MetricTransfer;
import com.linkedin.thirdeye.detector.metric.transfer.ScalingFactor;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import com.linkedin.thirdeye.util.TimeSeriesUtils;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.ISODateTimeFormat;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import static com.linkedin.thirdeye.anomaly.detection.lib.AutotuneMethodType.*;


@Path(value = "/dashboard")
@Produces(MediaType.APPLICATION_JSON)
public class AnomalyResource {
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyResource.class);
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String DEFAULT_CRON = "0 0 0 * * ?";
  private static final String UTF8 = "UTF-8";
  private static final String DEFAULT_FUNCTION_TYPE = "WEEK_OVER_WEEK_RULE";

  private AnomalyFunctionManager anomalyFunctionDAO;
  private MergedAnomalyResultManager anomalyMergedResultDAO;
  private RawAnomalyResultManager rawAnomalyResultDAO;
  private AlertConfigManager emailConfigurationDAO;
  private MetricConfigManager metricConfigDAO;
  private MergedAnomalyResultManager mergedAnomalyResultDAO;
  private AutotuneConfigManager autotuneConfigDAO;
  private DatasetConfigManager datasetConfigDAO;
  private AnomalyFunctionFactory anomalyFunctionFactory;
  private AlertFilterFactory alertFilterFactory;
  private LoadingCache<String, Long> collectionMaxDataTimeCache;
  private LoadingCache<String, String> dimensionFiltersCache;

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  public AnomalyResource(AnomalyFunctionFactory anomalyFunctionFactory, AlertFilterFactory alertFilterFactory) {
    this.anomalyFunctionDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
    this.rawAnomalyResultDAO = DAO_REGISTRY.getRawAnomalyResultDAO();
    this.anomalyMergedResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    this.emailConfigurationDAO = DAO_REGISTRY.getAlertConfigDAO();
    this.metricConfigDAO = DAO_REGISTRY.getMetricConfigDAO();
    this.mergedAnomalyResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    this.autotuneConfigDAO = DAO_REGISTRY.getAutotuneConfigDAO();
    this.datasetConfigDAO = DAO_REGISTRY.getDatasetConfigDAO();
    this.anomalyFunctionFactory = anomalyFunctionFactory;
    this.alertFilterFactory = alertFilterFactory;
    this.collectionMaxDataTimeCache = CACHE_REGISTRY_INSTANCE.getDatasetMaxDataTimeCache();
    this.dimensionFiltersCache = CACHE_REGISTRY_INSTANCE.getDimensionFiltersCache();
  }

  /************** CRUD for anomalies of a collection ********************************************************/
  @GET
  @Path("/anomalies/metrics")
  public List<String> viewMetricsForDataset(@QueryParam("dataset") String dataset) {
    if (StringUtils.isBlank(dataset)) {
      throw new IllegalArgumentException("dataset is a required query param");
    }
    List<String> metrics = anomalyFunctionDAO.findDistinctTopicMetricsByCollection(dataset);
    return metrics;
  }

  @GET
  @Path("/anomalies/view/{anomaly_merged_result_id}")
  public MergedAnomalyResultDTO getMergedAnomalyDetail(
      @NotNull @PathParam("anomaly_merged_result_id") long mergedAnomalyId) {
    return anomalyMergedResultDAO.findById(mergedAnomalyId);
  }

  // View merged anomalies for collection
  @GET
  @Path("/anomalies/view")
  public List<MergedAnomalyResultDTO> viewMergedAnomaliesInRange(@NotNull @QueryParam("dataset") String dataset,
      @QueryParam("startTimeIso") String startTimeIso, @QueryParam("endTimeIso") String endTimeIso,
      @QueryParam("metric") String metric, @QueryParam("dimensions") String exploredDimensions,
      @DefaultValue("true") @QueryParam("applyAlertFilter") boolean applyAlertFiler) {

    if (StringUtils.isBlank(dataset)) {
      throw new IllegalArgumentException("dataset is a required query param");
    }

    DateTime endTime = DateTime.now();
    if (StringUtils.isNotEmpty(endTimeIso)) {
      endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso);
    }
    DateTime startTime = endTime.minusDays(7);
    if (StringUtils.isNotEmpty(startTimeIso)) {
      startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso);
    }
    List<MergedAnomalyResultDTO> anomalyResults = new ArrayList<>();
    try {
      if (StringUtils.isNotBlank(exploredDimensions)) {
        // Decode dimensions map from request, which may contain encode symbols such as "%20D", etc.
        exploredDimensions = URLDecoder.decode(exploredDimensions, UTF8);
        try {
          // Ensure the dimension names are sorted in order to match the string in backend database
          DimensionMap sortedDimensions = OBJECT_MAPPER.readValue(exploredDimensions, DimensionMap.class);
          exploredDimensions = OBJECT_MAPPER.writeValueAsString(sortedDimensions);
        } catch (IOException e) {
          LOG.warn("exploreDimensions may not be sorted because failed to read it as a json string: {}", e.toString());
        }
      }

      boolean loadRawAnomalies = false;

      if (StringUtils.isNotBlank(metric)) {
        if (StringUtils.isNotBlank(exploredDimensions)) {
          anomalyResults =
              anomalyMergedResultDAO.findByCollectionMetricDimensionsTime(dataset, metric, exploredDimensions,
                  startTime.getMillis(), endTime.getMillis(), loadRawAnomalies);
        } else {
          anomalyResults = anomalyMergedResultDAO.findByCollectionMetricTime(dataset, metric, startTime.getMillis(),
              endTime.getMillis(), loadRawAnomalies);
        }
      } else {
        anomalyResults =
            anomalyMergedResultDAO.findByCollectionTime(dataset, startTime.getMillis(), endTime.getMillis(),
                loadRawAnomalies);
      }
    } catch (Exception e) {
      LOG.error("Exception in fetching anomalies", e);
    }

    if (applyAlertFiler) {
      // TODO: why need try catch?
      try {
        anomalyResults = AlertFilterHelper.applyFiltrationRule(anomalyResults, alertFilterFactory);
      } catch (Exception e) {
        LOG.warn("Failed to apply alert filters on anomalies for dataset:{}, metric:{}, start:{}, end:{}, exception:{}",
            dataset, metric, startTimeIso, endTimeIso, e);
      }
    }

    return anomalyResults;
  }

  // Get anomaly score
  @GET
  @Path("/anomalies/score/{anomaly_merged_result_id}")
  public double getAnomalyScore(@NotNull @PathParam("anomaly_merged_result_id") long mergedAnomalyId) {
    MergedAnomalyResultDTO mergedAnomaly = anomalyMergedResultDAO.findById(mergedAnomalyId);
    BaseAlertFilter alertFilter = new DummyAlertFilter();
    if (mergedAnomaly != null) {
      AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(mergedAnomaly.getFunctionId());
      alertFilter = alertFilterFactory.fromSpec(anomalyFunctionSpec.getAlertFilter());
    }
    return alertFilter.getProbability(mergedAnomaly);
  }

  //View raw anomalies for collection
  @GET
  @Path("/raw-anomalies/view")
  @Produces(MediaType.APPLICATION_JSON)
  public String viewRawAnomaliesInRange(@QueryParam("functionId") String functionId,
      @QueryParam("dataset") String dataset, @QueryParam("startTimeIso") String startTimeIso,
      @QueryParam("endTimeIso") String endTimeIso, @QueryParam("metric") String metric) throws JsonProcessingException {

    if (StringUtils.isBlank(functionId) && StringUtils.isBlank(dataset)) {
      throw new IllegalArgumentException("must provide dataset or functionId");
    }
    DateTime endTime = DateTime.now();
    if (StringUtils.isNotEmpty(endTimeIso)) {
      endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso);
    }
    DateTime startTime = endTime.minusDays(7);
    if (StringUtils.isNotEmpty(startTimeIso)) {
      startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso);
    }

    List<RawAnomalyResultDTO> rawAnomalyResults = new ArrayList<>();
    if (StringUtils.isNotBlank(functionId)) {
      rawAnomalyResults = rawAnomalyResultDAO.
          findAllByTimeAndFunctionId(startTime.getMillis(), endTime.getMillis(), Long.valueOf(functionId));
    } else if (StringUtils.isNotBlank(dataset)) {
      List<AnomalyFunctionDTO> anomalyFunctions = anomalyFunctionDAO.findAllByCollection(dataset);
      List<Long> functionIds = new ArrayList<>();
      for (AnomalyFunctionDTO anomalyFunction : anomalyFunctions) {
        if (StringUtils.isNotBlank(metric) && !anomalyFunction.getTopicMetric().equals(metric)) {
          continue;
        }
        functionIds.add(anomalyFunction.getId());
      }
      for (Long id : functionIds) {
        rawAnomalyResults.addAll(rawAnomalyResultDAO.
            findAllByTimeAndFunctionId(startTime.getMillis(), endTime.getMillis(), id));
      }
    }
    String response = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(rawAnomalyResults);
    return response;
  }

  /************* CRUD for anomaly functions of collection **********************************************/
  // View all anomaly functions
  @GET
  @Path("/anomaly-function")
  public List<AnomalyFunctionDTO> viewAnomalyFunctions(@NotNull @QueryParam("dataset") String dataset,
      @QueryParam("metric") String metric) {

    if (StringUtils.isBlank(dataset)) {
      throw new IllegalArgumentException("dataset is a required query param");
    }

    List<AnomalyFunctionDTO> anomalyFunctionSpecs = anomalyFunctionDAO.findAllByCollection(dataset);
    List<AnomalyFunctionDTO> anomalyFunctions = anomalyFunctionSpecs;

    if (StringUtils.isNotEmpty(metric)) {
      anomalyFunctions = new ArrayList<>();
      for (AnomalyFunctionDTO anomalyFunctionSpec : anomalyFunctionSpecs) {
        if (metric.equals(anomalyFunctionSpec.getTopicMetric())) {
          anomalyFunctions.add(anomalyFunctionSpec);
        }
      }
    }
    return anomalyFunctions;
  }

  /**
   * Endpoint to be used for creating new anomaly function
   * @param dataset name of dataset for the new anomaly function
   * @param functionName name of the new anomaly function. Should follow convention "productName_metricName_dimensionName_other"
   * @param metric name of metric on Thirdeye of the new anomaly function
   * @param metric_function was using as a consolidation function, now by default use "SUM"
   * @param type type of anomaly function. Minutely metrics use SIGN_TEST_VANILLA,
   *             Hourly metrics use REGRESSION_GAUSSIAN_SCAN, Daily metrics use SPLINE_REGRESSION
   * @param windowSize Detection window size
   * @param windowUnit Detection window unit
   * @param windowDelay Detection window delay (wait for data completeness)
   * @param windowDelayUnit Detection window delay unit
   * @param cron cron of detection
   * @param exploreDimensions explore dimensions in JSON
   * @param filters filters on dimensions
   * @param userInputDataGranularity user input metric granularity, if null uses dataset granularity
   * @param properties properties for anomaly function
   * @param isActive whether set the new anomaly function to be active or not
   * @return new anomaly function Id if successfully create new anomaly function
   * @throws Exception
   */
  @POST
  @Path("/anomaly-function")
  public Response createAnomalyFunction(@NotNull @QueryParam("dataset") String dataset,
      @NotNull @QueryParam("functionName") String functionName, @NotNull @QueryParam("metric") String metric,
      @NotNull @QueryParam("metricFunction") String metric_function, @QueryParam("type") String type,
      @NotNull @QueryParam("windowSize") String windowSize, @NotNull @QueryParam("windowUnit") String windowUnit,
      @QueryParam("windowDelay") @DefaultValue("0") String windowDelay, @QueryParam("cron") String cron,
      @QueryParam("windowDelayUnit") String windowDelayUnit, @QueryParam("exploreDimension") String exploreDimensions,
      @QueryParam("filters") String filters, @QueryParam("dataGranularity") String userInputDataGranularity,
      @NotNull @QueryParam("properties") String properties, @QueryParam("isActive") boolean isActive) throws Exception {

    if (StringUtils.isEmpty(dataset) || StringUtils.isEmpty(functionName) || StringUtils.isEmpty(metric)
        || StringUtils.isEmpty(windowSize) || StringUtils.isEmpty(windowUnit) || properties == null) {
      throw new IllegalArgumentException(String.format("Received nulll or emtpy String for one of the mandatory params: "
          + "dataset: %s, metric: %s, functionName %s, windowSize: %s, windowUnit: %s, and properties: %s", dataset,
          metric, functionName, windowSize, windowUnit, properties));
    }

    TimeGranularity dataGranularity;
    DatasetConfigDTO datasetConfig = DAO_REGISTRY.getDatasetConfigDAO().findByDataset(dataset);
    if (datasetConfig == null) {
      throw new IllegalArgumentException(String.format("No entry with dataset name %s exists", dataset));
    }
    if (userInputDataGranularity == null) {
      TimeSpec timespec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);
      dataGranularity = timespec.getDataGranularity();
    } else {
      dataGranularity = TimeGranularity.fromString(userInputDataGranularity);
    }

    AnomalyFunctionDTO anomalyFunctionSpec = new AnomalyFunctionDTO();
    anomalyFunctionSpec.setActive(isActive);
    anomalyFunctionSpec.setMetricFunction(MetricAggFunction.valueOf(metric_function));
    anomalyFunctionSpec.setCollection(dataset);
    anomalyFunctionSpec.setFunctionName(functionName);
    anomalyFunctionSpec.setTopicMetric(metric);
    anomalyFunctionSpec.setMetrics(Arrays.asList(metric));
    if (StringUtils.isEmpty(type)) {
      type = DEFAULT_FUNCTION_TYPE;
    }
    anomalyFunctionSpec.setType(type);
    anomalyFunctionSpec.setWindowSize(Integer.valueOf(windowSize));
    anomalyFunctionSpec.setWindowUnit(TimeUnit.valueOf(windowUnit));

    // Setting window delay time / unit
    TimeUnit dataGranularityUnit = dataGranularity.getUnit();

    // default windowDelay = 0, can be adjusted to cope with expected delay for given dataset/metric
    int windowDelayTime = Integer.valueOf(windowDelay);
    TimeUnit windowDelayTimeUnit;
    switch (dataGranularityUnit) {
      case MINUTES:
        windowDelayTimeUnit = TimeUnit.MINUTES;
        break;
      case DAYS:
        windowDelayTimeUnit = TimeUnit.DAYS;
        break;
      case HOURS:
      default:
        windowDelayTimeUnit = TimeUnit.HOURS;
    }
    if (StringUtils.isNotBlank(windowDelayUnit)) {
      windowDelayTimeUnit = TimeUnit.valueOf(windowDelayUnit.toUpperCase());
    }
    anomalyFunctionSpec.setWindowDelayUnit(windowDelayTimeUnit);
    anomalyFunctionSpec.setWindowDelay(windowDelayTime);

    // setup detection frequency if it's minutely function
    // the default frequency in AnomalyDetectionFunctionBean is 1 hour
    // when detection job is scheduled, the frequency may also be modified by data granularity
    // this frequency is a override to allow longer time period (lower frequency) and reduce system load
    if (dataGranularity.getUnit().equals(TimeUnit.MINUTES)) {
      TimeGranularity frequency = new TimeGranularity(15, TimeUnit.MINUTES);
      anomalyFunctionSpec.setFrequency(frequency);
    }

    // bucket size and unit are defaulted to the collection granularity
    anomalyFunctionSpec.setBucketSize(dataGranularity.getSize());
    anomalyFunctionSpec.setBucketUnit(dataGranularity.getUnit());

    if (StringUtils.isNotEmpty(exploreDimensions)) {
      anomalyFunctionSpec.setExploreDimensions(FunctionCreationUtils.getDimensions(datasetConfig, exploreDimensions));
    }
    if (!StringUtils.isBlank(filters)) {
      filters = URLDecoder.decode(filters, UTF8);
      String filterString = ThirdEyeUtils.getSortedFiltersFromJson(filters);
      anomalyFunctionSpec.setFilters(filterString);
    }
    anomalyFunctionSpec.setProperties(properties);

    if (StringUtils.isEmpty(cron)) {
      cron = DEFAULT_CRON;
    } else {
      // validate cron
      if (!CronExpression.isValidExpression(cron)) {
        throw new IllegalArgumentException("Invalid cron expression for cron : " + cron);
      }
    }
    anomalyFunctionSpec.setCron(cron);

    Long id = anomalyFunctionDAO.save(anomalyFunctionSpec);
    return Response.ok(id).build();
  }

  /**
   * Edit Anomaly function
   * @param id
   *    the id of the anomaly function to be updated
   * @param dataset
   *    the dataset to be monitored
   * @param metric
   *    the metric to be monitored
   * @param functionName
   *    the function name of the anomaly detection function
   * @param type
   *    the type of algorithm to be used in the detection function
   * @param windowSize
   *    the size of the monitoring window
   * @param windowUnit
   *    the unit of the monitoring window
   * @param windowDelay
   *    the delay of the monitoring window, that is the delay after the cron.
   *    ex: cron is set to every day at 10AM and the dealy is 1 hour, and the detection is triggered at 11am
   * @param windowDelayUnit
   *    the unit of the window delay
   * @param cron
   *    the schedule time of the detection, ref: http://www.cronmaker.com/
   * @param exploreDimensions
   *    the dimension to be drilled down for detection. Dimensions are separated by comma, e.g. dim1,dim2,dim3
   * @param filters
   *    the filter of the metric time series. the filter defines the domain of the time series.
   *    For example, filter is set to US and exploreDimensions is on pagekeys, then the detection is run on all pagekeys
   *    in the US
   * @param properties
   *    the properties of the detection function. The anomaly detection takes user-defined parameters via the properties.
   * @param isOverrideProp
   *    whether override whole properties using input properties (won't be useful if input properties is null)
   * @param isActive
   *    TRUE if the anomaly function is ready to be scheduled.
   * @param frequency
   *    The frequency is currently used to align the monitoring window. For example, frequency is 15 min, then the window
   *    is set to [0, 15), [15, 30), [30, 45) and [45, 0/60)
   * @return
   *    200 OK if the function is successfully updated
   * @throws Exception
   */
  @PUT
  @Path("/anomaly-function/{id}")
  public Response updateAnomalyFunction(@NotNull @PathParam("id") Long id, @QueryParam("dataset") String dataset,
      @QueryParam("functionName") String functionName, @QueryParam("metric") String metric,
      @QueryParam("type") String type, @QueryParam("windowSize") String windowSize,
      @QueryParam("windowUnit") String windowUnit, @QueryParam("windowDelay") String windowDelay,
      @QueryParam("cron") String cron, @QueryParam("windowDelayUnit") String windowDelayUnit,
      @QueryParam("exploreDimension") String exploreDimensions, @QueryParam("filters") String filters,
      @QueryParam("properties") String properties, @QueryParam("isOverrideProperties") Boolean isOverrideProp,
      @QueryParam("isActive") Boolean isActive, @QueryParam("frequency") String frequency,
      @QueryParam("bucket") String userInputDataGranularity) throws Exception {

    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(id);
    if (anomalyFunctionSpec == null) {
      throw new IllegalStateException("AnomalyFunctionSpec with id " + id + " does not exist");
    }

    // Update dataset if exists
    if (StringUtils.isNotBlank(dataset)) {
      DatasetConfigDTO datasetConfig = DAO_REGISTRY.getDatasetConfigDAO().findByDataset(dataset);
      TimeSpec timespec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);
      TimeGranularity dataGranularity = timespec.getDataGranularity();
      anomalyFunctionSpec.setCollection(dataset);

      // bucket size and unit are defaulted to the collection granularity
      anomalyFunctionSpec.setBucketSize(dataGranularity.getSize());
      anomalyFunctionSpec.setBucketUnit(dataGranularity.getUnit());
    }

    if (isActive != null) {
      anomalyFunctionSpec.setActive(isActive);
    }
    if (StringUtils.isNotBlank(functionName)) {
      anomalyFunctionSpec.setFunctionName(functionName);
    }
    if (StringUtils.isNotBlank(metric)) {
      anomalyFunctionSpec.setTopicMetric(metric);
      anomalyFunctionSpec.setMetrics(Arrays.asList(metric));
    }
    if (StringUtils.isNotEmpty(type)) {
      anomalyFunctionSpec.setType(type);
    }
    if (StringUtils.isNotBlank(windowSize)) {
      anomalyFunctionSpec.setWindowSize(Integer.valueOf(windowSize));
    }
    if (StringUtils.isNotBlank(windowUnit)) {
      anomalyFunctionSpec.setWindowUnit(TimeUnit.valueOf(windowUnit));
    }
    // window delay
    if (StringUtils.isNotBlank(windowDelay)) {
      anomalyFunctionSpec.setWindowDelay(Integer.valueOf(windowDelay));
    }
    if (StringUtils.isNotBlank(windowDelayUnit)) {
      anomalyFunctionSpec.setWindowDelayUnit(TimeUnit.valueOf(windowDelayUnit));
    }

    if (StringUtils.isNotBlank(filters)) {
      filters = URLDecoder.decode(filters, UTF8);
      String filterString = ThirdEyeUtils.getSortedFiltersFromJson(filters);
      anomalyFunctionSpec.setFilters(filterString);
    }
    if (StringUtils.isNotBlank(properties)) {
      Properties propertiesToUpdate = AnomalyFunctionDTO.toProperties(properties);
      Map<String, String> propertyMap = new HashMap<>();
      for (String propertyName : propertiesToUpdate.stringPropertyNames()) {
        propertyMap.put(propertyName, propertiesToUpdate.getProperty(propertyName));
      }
      if (isOverrideProp != null && isOverrideProp) {
        anomalyFunctionSpec.setProperties(new String(""));
      }
      anomalyFunctionSpec.updateProperties(propertyMap);
    }

    if (StringUtils.isNotEmpty(exploreDimensions)) {
      // Ensure that the explore dimension names are ordered as schema dimension names
      DatasetConfigDTO datasetConfig = datasetConfigDAO.findByDataset(anomalyFunctionSpec.getCollection());
      anomalyFunctionSpec.setExploreDimensions(FunctionCreationUtils.getDimensions(datasetConfig, exploreDimensions));
    }
    if (StringUtils.isNotEmpty(cron)) {
      // validate cron
      if (!CronExpression.isValidExpression(cron)) {
        throw new IllegalArgumentException("Invalid cron expression for cron : " + cron);
      }
      anomalyFunctionSpec.setCron(cron);
    }

    if (userInputDataGranularity != null) {
      // Update bucket size and unit
      TimeGranularity timeGranularity = TimeGranularity.fromString(userInputDataGranularity);
      if (timeGranularity != null) {
        anomalyFunctionSpec.setBucketSize(timeGranularity.getSize());
        anomalyFunctionSpec.setBucketUnit(timeGranularity.getUnit());
      } else {
        LOG.warn("Non-feasible time granularity expression: {}", userInputDataGranularity);
        return Response.status(Response.Status.BAD_REQUEST)
            .entity("Non-feasible time granularity expression: " + userInputDataGranularity)
            .build();
      }
    }

    if (StringUtils.isNotEmpty(frequency)) {
      // frequency string should be in the format of "5_MINUTES", "1_HOURS"
      anomalyFunctionSpec.setFrequency(TimeGranularity.fromString(frequency));
    }

    anomalyFunctionDAO.update(anomalyFunctionSpec);
    return Response.ok(id).build();
  }

  /**
   * Enable apply self-defined alert filter to anomaly function
   * @param id functionId to be updated
   * @param alertFilter alert filter in JSON format, for example:
   *       {"features":["window_size_in_hour,weight"],"intercept":["0,0"],"pattern":["UP,DOWN"],"threshold":["0.01,0.01"],"sensitivity":["MEDIUM"],"type":["alpha_beta_logistic_two_side"],"slope":["0,0"],"beta":["1,1"]}
   * @return OK if successfully applied alert filter
   */
  @PUT
  @Path("/anomaly-function/{id}/alert-filter")
  public Response applyAlertFilter(@NotNull @PathParam("id") Long id, @QueryParam("alertfilter") String alertFilter) {
    Map<String, String> alertFilterConfig = new HashMap<>();
    try {
      JSONObject alertFilterJSON = new JSONObject(alertFilter);
      Iterator<String> alertFilterKeys = alertFilterJSON.keys();
      while (alertFilterKeys.hasNext()) {
        String field = alertFilterKeys.next();
        JSONArray paramArray = alertFilterJSON.getJSONArray(field);
        alertFilterConfig.put(field, paramArray.get(0).toString());
      }
    } catch (JSONException e) {
      throw new IllegalArgumentException(String.format("Failed to apply alert filter!, {}", e.getMessage()));
    }
    AnomalyFunctionDTO targetFunction = anomalyFunctionDAO.findById(id);
    if (!alertFilterConfig.isEmpty()) {
      targetFunction.setAlertFilter(alertFilterConfig);
      anomalyFunctionDAO.update(targetFunction);
    }
    return Response.ok(id).build();
  }

  /**
   * Apply an autotune configuration to an existing function
   * @param id
   * The id of an autotune configuration
   * @param isCloneFunction
   * Should we clone the function or simply apply the autotune configuration to the existing function
   * @return
   * an activated anomaly detection function
   */
  @POST
  @Path("/anomaly-function/apply/{autotuneConfigId}")
  public Response applyAutotuneConfig(@PathParam("autotuneConfigId") @NotNull long id,
      @QueryParam("cloneFunction") @DefaultValue("false") boolean isCloneFunction,
      @QueryParam("cloneAnomalies") Boolean isCloneAnomalies) {
    Map<String, String> responseMessage = new HashMap<>();
    AutotuneConfigDTO autotuneConfigDTO = autotuneConfigDAO.findById(id);
    if (autotuneConfigDTO == null) {
      responseMessage.put("message", "Cannot find the autotune configuration entry " + id + ".");
      return Response.status(Response.Status.BAD_REQUEST).entity(responseMessage).build();
    }
    if (autotuneConfigDTO.getConfiguration() == null || autotuneConfigDTO.getConfiguration().isEmpty()) {
      responseMessage.put("message",
          "Autotune configuration is null or empty. The original function is optimal. Nothing to change");
      return Response.ok(responseMessage).build();
    }

    if (isCloneAnomalies == null) { // if isCloneAnomalies is not given, assign a default value
      isCloneAnomalies = containsLabeledAnomalies(autotuneConfigDTO.getFunctionId());
    }

    AnomalyFunctionDTO originalFunction = anomalyFunctionDAO.findById(autotuneConfigDTO.getFunctionId());
    AnomalyFunctionDTO targetFunction = originalFunction;

    // clone anomaly function and its anomaly results if requested
    if (isCloneFunction) {
      OnboardResource onboardResource =
          new OnboardResource(anomalyFunctionDAO, mergedAnomalyResultDAO, rawAnomalyResultDAO);
      long cloneId;
      String tag = "clone";
      try {
        cloneId = onboardResource.cloneAnomalyFunctionById(originalFunction.getId(), "clone", isCloneAnomalies);
      } catch (Exception e) {
        responseMessage.put("message",
            "Unable to clone function " + originalFunction.getId() + " with clone tag \"clone\"");
        LOG.warn("Unable to clone function {} with clone tag \"{}\"", originalFunction.getId(), "clone");
        return Response.status(Response.Status.CONFLICT).entity(responseMessage).build();
      }
      targetFunction = anomalyFunctionDAO.findById(cloneId);
    }

    // Verify if to update alert filter or function configuraions
    // if auto tune method is EXHAUSTIVE, which belongs to function auto tune, need to update function configurations
    // if auto tune method is ALERT_FILTER_LOGISITC_AUTO_TUNE or INITIATE_ALERT_FILTER_LOGISTIC_AUTO_TUNE, alert filter is to be updated
    if (autotuneConfigDTO.getAutotuneMethod() != EXHAUSTIVE) {
      targetFunction.setAlertFilter(autotuneConfigDTO.getConfiguration());
    } else {
      // Update function configuration
      targetFunction.updateProperties(autotuneConfigDTO.getConfiguration());
    }
    targetFunction.setActive(true);
    anomalyFunctionDAO.update(targetFunction);

    // Deactivate original function
    if (isCloneFunction) {
      originalFunction.setActive(false);
      anomalyFunctionDAO.update(originalFunction);
    }

    return Response.ok(targetFunction).build();
  }

  /**
   * Get the timeseries with function baseline for an anomaly function.
   * The function baseline can be 1) the baseline for online analysis, including baseline in training and testing range,
   * and 2) the baseline for offline analysis, where only training time rage is included.
   *
   * Online Analysis
   *  This mode take the monitoring window as the testing window; the training time range is requested based on the
   *  given testing window. The function returns the baseline and observed values in both testing and training window.
   *  Note that, different from offline analysis, the size of the training window is fixed. Users are able to trim the
   *  window, but not extend.
   *
   * Offline Analysis
   *  This mode take the monitoring window as the training window, while the testing window is empty. The function only
   *  fits the time series in training window. The fitted time series is then returned.
   *
   * For the data points removed because of anomalies, the second part of this function will make up the time series by
   * the information provided by the anomalies.
   *
   * The baseline is generated from getTimeSeriesView() in each detection function.
   * @param functionId
   *      the target anomaly function
   * @param startTimeIso
   *      the start time of the monitoring window (inclusive)
   * @param endTimeIso
   *      the end time of the monitoring window (exclusive)
   * @param mode
   *      the mode of the baseline calculation
   * @param dimension
   *      the dimension string in JSON format
   * @return
   *      the Map that maps dimension string to the AnomalyTimelinesView
   * @throws Exception
   */
  @GET
  @Path(value = "/anomaly-function/{id}/baseline")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTimeSeriesAndBaselineData(@PathParam("id") long functionId,
      @QueryParam("start") String startTimeIso, @QueryParam("end") String endTimeIso,
      @QueryParam("mode") @DefaultValue("ONLINE") String mode,
      @QueryParam("dimension") @NotNull String dimension) throws Exception {
    AnomalyFunctionDTO anomalyFunctionSpec = DAO_REGISTRY.getAnomalyFunctionDAO().findById(functionId);
    if (anomalyFunctionSpec == null) {
      LOG.warn("Cannot find anomaly function {}", functionId);
      return Response.status(Response.Status.BAD_REQUEST).entity("Cannot find anomaly function " + functionId).build();
    }

    TimeGranularity bucketTimeGranularity =
        new TimeGranularity(anomalyFunctionSpec.getBucketSize(), anomalyFunctionSpec.getBucketUnit());
    // If startTime is not assigned, use the max date time as the start time
    DateTime startTime = new DateTime(collectionMaxDataTimeCache.get(anomalyFunctionSpec.getCollection()));
    DateTime endTime = startTime.plus(bucketTimeGranularity.toPeriod());
    if (StringUtils.isNotBlank(startTimeIso)) {
      startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso);
    }
    if (StringUtils.isNotBlank(endTimeIso)) {
      endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso);
    }

    if (startTime.isAfter(endTime)) {
      LOG.warn("Start time {} is after end time {}", startTime, endTime);
      return Response.status(Response.Status.BAD_REQUEST).entity("Start time is after end time").build();
    }

    // Get DimensionMap from input dimension String
    Multimap<String, String> functionFilters = anomalyFunctionSpec.getFilterSet();
    String[] exploreDimensions = anomalyFunctionSpec.getExploreDimensions().split(",");
    Map<String, String> inputDimension = OBJECT_MAPPER.readValue(dimension, Map.class);
    DimensionMap dimensionsToBeEvaluated = new DimensionMap();
    for (String exploreDimension : exploreDimensions) {
      if (!inputDimension.containsKey(exploreDimension)) {
        String msg = String.format("Query %s doesn't specify the value of explore dimension %s", dimension, exploreDimension);
        LOG.error(msg);
        throw new WebApplicationException(msg);
      } else {
        dimensionsToBeEvaluated.put(exploreDimension, inputDimension.get(exploreDimension));
      }
    }

    // Get the anomaly time lines view of the anomaly function on each dimension
    HashMap<String, AnomalyTimelinesView> dimensionMapAnomalyTimelinesViewMap = new HashMap<>();
    AnomaliesResource anomaliesResource = new AnomaliesResource(anomalyFunctionFactory, alertFilterFactory);
    DatasetConfigDTO datasetConfigDTO = datasetConfigDAO.findByDataset(anomalyFunctionSpec.getCollection());
    AnomalyTimelinesView anomalyTimelinesView = null;
    if (mode.equalsIgnoreCase("ONLINE")) {
      // If online, use the monitoring time window to query time range and generate the baseline
      anomalyTimelinesView =
          anomaliesResource.getTimelinesViewInMonitoringWindow(anomalyFunctionSpec, datasetConfigDTO, startTime,
              endTime, dimensionsToBeEvaluated);
    } else {
      // If offline, request baseline in user-defined data range
      List<Pair<Long, Long>> dataRangeIntervals = new ArrayList<>();
      dataRangeIntervals.add(new Pair<Long, Long>(endTime.getMillis(), endTime.getMillis()));
      dataRangeIntervals.add(new Pair<Long, Long>(startTime.getMillis(), endTime.getMillis()));
      anomalyTimelinesView =
          anomaliesResource.getTimelinesViewInMonitoringWindow(anomalyFunctionSpec, datasetConfigDTO,
              dataRangeIntervals, dimensionsToBeEvaluated);
    }
    anomalyTimelinesView = amendAnomalyTimelinesViewWithAnomalyResults(anomalyFunctionSpec, anomalyTimelinesView,
        dimensionsToBeEvaluated);

    return Response.ok(anomalyTimelinesView).build();
  }

  /**
   * Amend the anomaly time lines view with the information in anomaly results
   *  - if the anomaly result contains view, override the timeseries by the view
   *  - if the anomaly result contains raw anomalies, override the timeseries by the current and baseline values inside
   *  - otherwise, use the merged anomaly information for all missing points
   * @param anomalyFunctionSpec
   * @param originalTimelinesView
   * @param dimensionMap
   * @return
   */
  public AnomalyTimelinesView amendAnomalyTimelinesViewWithAnomalyResults(AnomalyFunctionDTO anomalyFunctionSpec,
      AnomalyTimelinesView originalTimelinesView, DimensionMap dimensionMap) {
    if (originalTimelinesView == null || anomalyFunctionSpec == null) {
      LOG.error("AnomalyTimelinesView or AnomalyFunctionDTO is null");
      return null;
    }
    long bucketMillis =
        (new TimeGranularity(anomalyFunctionSpec.getBucketSize(), anomalyFunctionSpec.getBucketUnit())).toMillis();
    Tuple2<TimeSeries, TimeSeries> timeSeriesTuple = TimeSeriesUtils.toTimeSeries(originalTimelinesView);
    TimeSeries observed = timeSeriesTuple._1();
    TimeSeries expected = timeSeriesTuple._2();
    Interval timeSeriesInterval = observed.getTimeSeriesInterval();
    List<MergedAnomalyResultDTO> mergedAnomalyResults =
        mergedAnomalyResultDAO.findOverlappingByFunctionIdDimensions(anomalyFunctionSpec.getId(),
            timeSeriesInterval.getStartMillis(), timeSeriesInterval.getEndMillis(), dimensionMap.toString(), true);

    for (MergedAnomalyResultDTO mergedAnomalyResult : mergedAnomalyResults) {
      if (mergedAnomalyResult.getDimensions().equals(dimensionMap)) {
        String viewString = mergedAnomalyResult.getProperties().get("anomalyTimelinesView");
        // Strategy 1: override the timeseries by the view
        if (StringUtils.isNotBlank(viewString)) { // Update TimeSeries using the view in the anomaly result
          AnomalyTimelinesView anomalyView;
          try {
            anomalyView = AnomalyTimelinesView.fromJsonString(viewString);
          } catch (Exception e) {
            LOG.warn("Unable to fetch view from anomaly result; use anomaly result directly");
            break;
          }
          timeSeriesTuple = TimeSeriesUtils.toTimeSeries(anomalyView);
          TimeSeries anomalyObserved = timeSeriesTuple._1();
          TimeSeries anomalyExpected = timeSeriesTuple._2();

          for (long timestamp : anomalyObserved.timestampSet()) {
            // Preventing the append data points outside of view window
            if (!timeSeriesInterval.contains(timestamp)) {
              continue;
            }
            // align timestamp to the begin of the day if Bucket is in DAYS
            long indexTimestamp = timestamp;
            if (TimeUnit.DAYS.toMillis(1l) == bucketMillis) {
              DateTime timestampDateTime = new DateTime(timestamp);
              // the timestamp shifts ahead of correct timestamp because of start of DST
              if (timestampDateTime.minusHours(1).toLocalDate().equals(timestampDateTime.toLocalDate())) {
                timestamp = timestampDateTime.minusHours(1).getMillis();
              } else if (timestampDateTime.plusHours(1)
                  .toLocalDate()
                  .equals(timestampDateTime.plusDays(1).toLocalDate())) {
                timestamp = (new DateTime(timestamp)).plusDays(1).withTimeAtStartOfDay().getMillis();
              }
            }
            if (timeSeriesInterval.contains(timestamp)) {
              observed.set(timestamp, anomalyObserved.get(indexTimestamp));
              expected.set(timestamp, anomalyExpected.get(indexTimestamp));
            }
          }
          continue;
        }

        // Strategy 2: override the timeseries by the current and baseline values inside raw anomaly
        if (mergedAnomalyResult.getAnomalyResults().size() > 0) {
          for (RawAnomalyResultDTO rawAnomalyResult : mergedAnomalyResult.getAnomalyResults()) {
            if (!observed.hasTimestamp(rawAnomalyResult.getStartTime())) {
              // if the observed value in the timeseries is not removed, use the original observed value
              observed.set(rawAnomalyResult.getStartTime(), rawAnomalyResult.getAvgCurrentVal());
            }
            if (!expected.hasTimestamp(rawAnomalyResult.getStartTime())) {
              // if the expected value in the timeserie is not removed, use the original expected value
              expected.set(rawAnomalyResult.getStartTime(), rawAnomalyResult.getAvgBaselineVal());
            }
          }
        } else {
          // Strategy 3: use the merged anomaly information for all missing points
          for (long start = mergedAnomalyResult.getStartTime(); start < mergedAnomalyResult.getEndTime();
              start += bucketMillis) {
            if (!observed.hasTimestamp(mergedAnomalyResult.getStartTime())) {
              // if the observed value in the timeseries is not removed, use the original observed value
              observed.set(start, mergedAnomalyResult.getAvgCurrentVal());
            }
            if (!expected.hasTimestamp(mergedAnomalyResult.getStartTime())) {
              // if the expected value in the timeserie is not removed, use the original expected value
              expected.set(start, mergedAnomalyResult.getAvgBaselineVal());
            }
          }
        }
      }
    }
    return TimeSeriesUtils.toAnomalyTimeLinesView(observed, expected, bucketMillis);
  }

  /**
   * Enumerate all possible DimensionMap for a given anomaly detection function spec
   * @param anomalyFunctionSpec
   * @return
   */
  public List<DimensionMap> enumerateDimensionMapCombinations(AnomalyFunctionDTO anomalyFunctionSpec)
      throws ExecutionException {
    List<DimensionMap> dimensionMapEnumeration = new ArrayList<>();
    if (StringUtils.isBlank(anomalyFunctionSpec.getExploreDimensions())) {
      dimensionMapEnumeration.add(new DimensionMap());
      return dimensionMapEnumeration;
    }
    String jsonFilters = dimensionFiltersCache.get(anomalyFunctionSpec.getCollection());
    HashMap<String, List<String>> collectionFilters;
    try {
      collectionFilters = OBJECT_MAPPER.readValue(jsonFilters, HashMap.class);
    } catch (Exception e) {
      LOG.error("Unable to cast or parse json value: {}", jsonFilters, e);
      dimensionMapEnumeration.add(new DimensionMap());
      return dimensionMapEnumeration;
    }
    String[] exploreDimensions = anomalyFunctionSpec.getExploreDimensions().split(",");
    Multimap<String, String> functionFilters = anomalyFunctionSpec.getFilterSet();

    List<String> dimensionStringEnumeration = new ArrayList<>();
    dimensionStringEnumeration.add("");
    for (String exploreDimension : exploreDimensions) {
      if (!collectionFilters.containsKey(exploreDimension)) {
        continue;
      }
      // Get the full list of attributes from dataset metaData
      List<String> possibleAttributes = collectionFilters.get(exploreDimension);
      // If there is user pre-defined attributes, use user's definition
      if (functionFilters.containsKey(exploreDimension)) {
        possibleAttributes = new ArrayList<>(functionFilters.get(exploreDimension));
      }

      // Append new attribute to the dimension string
      List<String> targetEnumerations = dimensionStringEnumeration;
      dimensionStringEnumeration = new ArrayList<>();
      for (String targetEntry : targetEnumerations) {
        for (String attribute : possibleAttributes) {
          dimensionStringEnumeration.add(targetEntry + "," + exploreDimension + "=" + attribute);
        }
      }
    }

    for (String dimensionString : dimensionStringEnumeration) {
      // remove the first comma from the dimension string
      dimensionMapEnumeration.add(new DimensionMap("{" + dimensionString.substring(1) + "}"));
    }
    return dimensionMapEnumeration;
  }

  /**
   * Check if the given function contains labeled anomalies
   * @param functionId
   * an id of an anomaly detection function
   * @return
   * true if there are labeled anomalies detected by the function
   */
  private boolean containsLabeledAnomalies(long functionId) {
    List<MergedAnomalyResultDTO> mergedAnomalies = mergedAnomalyResultDAO.findByFunctionId(functionId, true);

    for (MergedAnomalyResultDTO mergedAnomaly : mergedAnomalies) {
      AnomalyFeedback feedback = mergedAnomaly.getFeedback();
      if (feedback == null) {
        continue;
      }
      if (feedback.getFeedbackType().equals(AnomalyFeedbackType.ANOMALY) || feedback.getFeedbackType()
          .equals(AnomalyFeedbackType.ANOMALY_NEW_TREND)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Show the content of merged anomalies whose start time is located in the given time ranges
   *
   * @param functionId id of the anomaly function
   * @param startTimeIso The start time of the monitoring window
   * @param endTimeIso The start time of the monitoring window
   * @param anomalyType type of anomaly: "raw" or "merged"
   * @param applyAlertFiler can choose apply alert filter when query merged anomaly results
   * @return list of anomalyIds (Long)
   */
  @GET
  @Path("anomaly-function/{id}/anomalies")
  public List<Long> getAnomaliesByFunctionId(@PathParam("id") Long functionId, @QueryParam("start") String startTimeIso,
      @QueryParam("end") String endTimeIso, @QueryParam("type") @DefaultValue("merged") String anomalyType,
      @QueryParam("apply-alert-filter") @DefaultValue("false") boolean applyAlertFiler,
      @QueryParam("useNotified") @DefaultValue("false") boolean useNotified) {

    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
    if (anomalyFunction == null) {
      LOG.info("Anomaly functionId {} is not found", functionId);
      return null;
    }

    long startTime;
    long endTime;
    try {
      startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeIso).getMillis();
      endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeIso).getMillis();
    } catch (Exception e) {
      throw new WebApplicationException(
          "Unable to parse strings, " + startTimeIso + " and " + endTimeIso + ", in ISO DateTime format", e);
    }

    LOG.info("Retrieving {} anomaly results for function {} in the time range: {} -- {}", anomalyType, functionId,
        startTimeIso, endTimeIso);

    ArrayList<Long> anomalyIdList = new ArrayList<>();
    if (anomalyType.equals("raw")) {
      List<RawAnomalyResultDTO> rawDTO = rawAnomalyResultDAO.findAllByTimeAndFunctionId(startTime, endTime, functionId);
      for (RawAnomalyResultDTO dto : rawDTO) {
        anomalyIdList.add(dto.getId());
      }
    } else if (anomalyType.equals("merged")) {
      List<MergedAnomalyResultDTO> mergedResults =
          mergedAnomalyResultDAO.findByStartTimeInRangeAndFunctionId(startTime, endTime, functionId, true);

      // apply alert filter
      if (applyAlertFiler) {
        try {
          mergedResults = AlertFilterHelper.applyFiltrationRule(mergedResults, alertFilterFactory);
        } catch (Exception e) {
          LOG.warn("Failed to apply alert filters on {} anomalies for function id:{}, start:{}, end:{}, exception:{}",
              anomalyType, functionId, startTimeIso, endTimeIso, e);
        }
      }

      for (MergedAnomalyResultDTO mergedAnomaly : mergedResults) {
        // if use notified flag, only keep anomalies isNotified == true
        if ( (useNotified && mergedAnomaly.isNotified()) || !useNotified
            || AnomalyResultSource.USER_LABELED_ANOMALY.equals(mergedAnomaly.getAnomalyResultSource())) {
          anomalyIdList.add(mergedAnomaly.getId());
        }
      }
    }

    return anomalyIdList;
  }

  // Activate anomaly function
  @POST
  @Path("/anomaly-function/activate")
  public Response activateAnomalyFunction(@NotNull @QueryParam("functionId") Long id) {
    if (id == null) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
    toggleFunctionById(id, true);
    return Response.ok(id).build();
  }

  // batch activate and deactivate anomaly functions
  @POST
  @Path("/anomaly-functions/activate")
  public String activateFunction(@QueryParam("functionIds") String functionIds) {
    toggleFunctions(functionIds, true);
    return functionIds;
  }

  @POST
  @Path("/anomaly-functions/deactivate")
  public String deactivateFunction(@QueryParam("functionIds") String functionIds) {
    toggleFunctions(functionIds, false);
    return functionIds;
  }

  /**
   * toggle anomaly functions to active and inactive
   *
   * @param functionIds string comma separated function ids, ALL meaning all functions
   * @param isActive boolean true or false, set function as true or false
   */
  private void toggleFunctions(String functionIds, boolean isActive) {
    List<Long> functionIdsList = new ArrayList<>();

    // can add tokens here to activate and deactivate all functions for example
    // functionIds == {SPECIAL TOKENS} --> functionIdsList = anomalyFunctionDAO.findAll()

    if (StringUtils.isNotBlank(functionIds)) {
      String[] tokens = functionIds.split(",");
      for (String token : tokens) {
        functionIdsList.add(Long.valueOf(token));  // unhandled exception is expected
      }
    }

    for (long id : functionIdsList) {
      toggleFunctionById(id, isActive);
    }
  }

  private void toggleFunctionById(long id, boolean isActive) {
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(id);
    anomalyFunctionSpec.setActive(isActive);
    anomalyFunctionDAO.update(anomalyFunctionSpec);
  }

  // Delete anomaly function
  @DELETE
  @Path("/anomaly-function")
  public Response deleteAnomalyFunctions(@NotNull @QueryParam("id") Long id,
      @QueryParam("functionName") String functionName) throws IOException {

    if (id == null) {
      throw new IllegalArgumentException("id is a required query param");
    }

    // call endpoint to shutdown if active
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(id);
    if (anomalyFunctionSpec == null) {
      throw new IllegalStateException("No anomalyFunctionSpec with id " + id);
    }

    // delete dependent entities
    // email config mapping
    List<AlertConfigDTO> emailConfigurations = emailConfigurationDAO.findByFunctionId(id);
    for (AlertConfigDTO emailConfiguration : emailConfigurations) {
      emailConfiguration.getEmailConfig().getFunctionIds().remove(anomalyFunctionSpec.getId());
      emailConfigurationDAO.update(emailConfiguration);
    }

    // raw result mapping
    List<RawAnomalyResultDTO> rawResults =
        rawAnomalyResultDAO.findAllByTimeAndFunctionId(0, System.currentTimeMillis(), id);
    for (RawAnomalyResultDTO result : rawResults) {
      rawAnomalyResultDAO.delete(result);
    }

    // merged anomaly mapping
    List<MergedAnomalyResultDTO> mergedResults = anomalyMergedResultDAO.findByFunctionId(id, true);
    for (MergedAnomalyResultDTO result : mergedResults) {
      anomalyMergedResultDAO.delete(result);
    }

    // delete from db
    anomalyFunctionDAO.deleteById(id);
    return Response.noContent().build();
  }

  /************ Anomaly Feedback **********/
  @GET
  @Path(value = "anomaly-result/feedback")
  @Produces(MediaType.APPLICATION_JSON)
  public AnomalyFeedbackType[] getAnomalyFeedbackTypes() {
    return AnomalyFeedbackType.values();
  }

  /**
   * @param anomalyResultId : anomaly merged result id
   * @param payload         : Json payload containing feedback @see com.linkedin.thirdeye.constant.AnomalyFeedbackType
   *                        eg. payload
   *                        <p/>
   *                        { "feedbackType": "NOT_ANOMALY", "comment": "this is not an anomaly" }
   */
  @POST
  @Path(value = "anomaly-merged-result/feedback/{anomaly_merged_result_id}")
  public void updateAnomalyMergedResultFeedback(@PathParam("anomaly_merged_result_id") long anomalyResultId,
      String payload) {
    try {
      MergedAnomalyResultDTO result = anomalyMergedResultDAO.findById(anomalyResultId);
      if (result == null) {
        throw new IllegalArgumentException("AnomalyResult not found with id " + anomalyResultId);
      }
      AnomalyFeedbackDTO feedbackRequest = OBJECT_MAPPER.readValue(payload, AnomalyFeedbackDTO.class);
      AnomalyFeedback feedback = result.getFeedback();
      if (feedback == null) {
        feedback = new AnomalyFeedbackDTO();
        result.setFeedback(feedback);
      }
      feedback.setComment(feedbackRequest.getComment());
      feedback.setFeedbackType(feedbackRequest.getFeedbackType());
      anomalyMergedResultDAO.updateAnomalyFeedback(result);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid payload " + payload, e);
    }
  }

  @POST
  @Path(value = "anomaly-result/feedback/{anomaly_result_id}")
  public void updateAnomalyResultFeedback(@PathParam("anomaly_result_id") long anomalyResultId, String payload) {
    try {
      RawAnomalyResultDTO result = rawAnomalyResultDAO.findById(anomalyResultId);
      if (result == null) {
        throw new IllegalArgumentException("AnomalyResult not found with id " + anomalyResultId);
      }
      AnomalyFeedbackDTO feedbackRequest = OBJECT_MAPPER.readValue(payload, AnomalyFeedbackDTO.class);
      AnomalyFeedbackDTO feedback = result.getFeedback();
      if (feedback == null) {
        feedback = new AnomalyFeedbackDTO();
        result.setFeedback(feedback);
      }
      feedback.setComment(feedbackRequest.getComment());
      feedback.setFeedbackType(feedbackRequest.getFeedbackType());

      rawAnomalyResultDAO.update(result);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid payload " + payload, e);
    }
  }

  /**
   * Returns the time series for the given anomaly.
   *
   * If viewWindowStartTime and/or viewWindowEndTime is not given, then a window is padded automatically. The padded
   * windows is half of the anomaly window size. For instance, if the anomaly lasts for 4 hours, then the pad window
   * size is 2 hours. The max padding size is 1 day.
   *
   * @param anomalyResultId the id of the given anomaly
   * @param viewWindowStartTime start time of the time series, inclusive
   * @param viewWindowEndTime end time of the time series, inclusive
   * @return the time series of the given anomaly
   * @throws Exception when it fails to retrieve collection, i.e., dataset, information
   */
  @GET
  @Path("/anomaly-merged-result/timeseries/{anomaly_merged_result_id}")
  public AnomalyTimelinesView getAnomalyMergedResultTimeSeries(
      @NotNull @PathParam("anomaly_merged_result_id") long anomalyResultId,
      @NotNull @QueryParam("aggTimeGranularity") String aggTimeGranularity,
      @QueryParam("start") long viewWindowStartTime, @QueryParam("end") long viewWindowEndTime) throws Exception {

    boolean loadRawAnomalies = false;
    MergedAnomalyResultDTO anomalyResult = anomalyMergedResultDAO.findById(anomalyResultId, loadRawAnomalies);
    Map<String, String> anomalyProps = anomalyResult.getProperties();

    AnomalyTimelinesView anomalyTimelinesView = null;

    // check if there is AnomalyTimelinesView in the Properties. If yes, use the AnomalyTimelinesView
    if (anomalyProps.containsKey("anomalyTimelinesView")) {
      anomalyTimelinesView = AnomalyTimelinesView.fromJsonString(anomalyProps.get("anomalyTimelinesView"));
    } else {

      DimensionMap dimensions = anomalyResult.getDimensions();
      AnomalyFunctionDTO anomalyFunctionSpec = anomalyResult.getFunction();
      BaseAnomalyFunction anomalyFunction = anomalyFunctionFactory.fromSpec(anomalyFunctionSpec);

      // Calculate view window start and end if they are not given by the user, which should be 0 if it is not given.
      // By default, the padding window size is half of the anomaly window.
      if (viewWindowStartTime == 0 || viewWindowEndTime == 0) {
        long anomalyWindowStartTime = anomalyResult.getStartTime();
        long anomalyWindowEndTime = anomalyResult.getEndTime();

        long bucketMillis =
            TimeUnit.MILLISECONDS.convert(anomalyFunctionSpec.getBucketSize(), anomalyFunctionSpec.getBucketUnit());
        long bucketCount = (anomalyWindowEndTime - anomalyWindowStartTime) / bucketMillis;
        long paddingMillis = Math.max(1, (bucketCount / 2)) * bucketMillis;
        if (paddingMillis > TimeUnit.DAYS.toMillis(1)) {
          paddingMillis = TimeUnit.DAYS.toMillis(1);
        }

        if (viewWindowStartTime == 0) {
          viewWindowStartTime = anomalyWindowStartTime - paddingMillis;
        }
        if (viewWindowEndTime == 0) {
          viewWindowEndTime = anomalyWindowEndTime + paddingMillis;
        }
      }

      TimeGranularity timeGranularity =
          Utils.getAggregationTimeGranularity(aggTimeGranularity, anomalyFunctionSpec.getCollection());
      long bucketMillis = timeGranularity.toMillis();
      // ThirdEye backend is end time exclusive, so one more bucket is appended to make end time inclusive for frontend.
      viewWindowEndTime += bucketMillis;

      long maxDataTime = collectionMaxDataTimeCache.get(anomalyResult.getCollection());
      if (viewWindowEndTime > maxDataTime) {
        viewWindowEndTime = (anomalyResult.getEndTime() > maxDataTime) ? anomalyResult.getEndTime() : maxDataTime;
      }

      DateTime viewWindowStart = new DateTime(viewWindowStartTime);
      DateTime viewWindowEnd = new DateTime(viewWindowEndTime);
      AnomalyDetectionInputContextBuilder anomalyDetectionInputContextBuilder =
          new AnomalyDetectionInputContextBuilder(anomalyFunctionFactory);
      anomalyDetectionInputContextBuilder.setFunction(anomalyFunctionSpec)
          .fetchTimeSeriesDataByDimension(viewWindowStart, viewWindowEnd, dimensions, false)
          .fetchScalingFactors(viewWindowStart, viewWindowEnd)
          .fetchExistingMergedAnomalies(viewWindowStart, viewWindowEnd, false);

      AnomalyDetectionInputContext adInputContext = anomalyDetectionInputContextBuilder.build();

      MetricTimeSeries metricTimeSeries = adInputContext.getDimensionMapMetricTimeSeriesMap().get(dimensions);

      if (metricTimeSeries == null) {
        // If this case happened, there was something wrong with anomaly detection because we are not able to retrieve
        // the timeseries for the given anomaly
        return new AnomalyTimelinesView();
      }

      // Transform time series with scaling factor
      List<ScalingFactor> scalingFactors = adInputContext.getScalingFactors();
      if (CollectionUtils.isNotEmpty(scalingFactors)) {
        Properties properties = anomalyFunction.getProperties();
        MetricTransfer.rescaleMetric(metricTimeSeries, viewWindowStartTime, scalingFactors,
            anomalyFunctionSpec.getTopicMetric(), properties);
      }

      List<MergedAnomalyResultDTO> knownAnomalies = adInputContext.getKnownMergedAnomalies().get(dimensions);
      // Known anomalies are ignored (the null parameter) because 1. we can reduce users' waiting time and 2. presentation
      // data does not need to be as accurate as the one used for detecting anomalies
      anomalyTimelinesView =
          anomalyFunction.getTimeSeriesView(metricTimeSeries, bucketMillis, anomalyFunctionSpec.getTopicMetric(),
              viewWindowStartTime, viewWindowEndTime, knownAnomalies);
    }

    // Generate summary for frontend
    List<TimeBucket> timeBuckets = anomalyTimelinesView.getTimeBuckets();
    if (timeBuckets.size() > 0) {
      TimeBucket firstBucket = timeBuckets.get(0);
      anomalyTimelinesView.addSummary("currentStart", Long.toString(firstBucket.getCurrentStart()));
      anomalyTimelinesView.addSummary("baselineStart", Long.toString(firstBucket.getBaselineStart()));

      TimeBucket lastBucket = timeBuckets.get(timeBuckets.size() - 1);
      anomalyTimelinesView.addSummary("currentEnd", Long.toString(lastBucket.getCurrentStart()));
      anomalyTimelinesView.addSummary("baselineEnd", Long.toString(lastBucket.getBaselineEnd()));
    }

    return anomalyTimelinesView;
  }

  @GET
  @Path("/external-dashboard-url/{mergedAnomalyId}")
  public String getExternalDashboardUrlForMergedAnomaly(@NotNull @PathParam("mergedAnomalyId") Long mergedAnomalyId)
      throws Exception {

    MergedAnomalyResultDTO mergedAnomalyResultDTO = mergedAnomalyResultDAO.findById(mergedAnomalyId);
    String metric = mergedAnomalyResultDTO.getMetric();
    String dataset = mergedAnomalyResultDTO.getCollection();
    Long startTime = mergedAnomalyResultDTO.getStartTime();
    Long endTime = mergedAnomalyResultDTO.getEndTime();
    MetricConfigDTO metricConfigDTO = metricConfigDAO.findByMetricAndDataset(metric, dataset);

    Map<String, String> context = new HashMap<>();
    context.put(MetricConfigBean.URL_TEMPLATE_START_TIME, String.valueOf(startTime));
    context.put(MetricConfigBean.URL_TEMPLATE_END_TIME, String.valueOf(endTime));
    StrSubstitutor strSubstitutor = new StrSubstitutor(context);
    Map<String, String> urlTemplates = metricConfigDTO.getExtSourceLinkInfo();
    for (Entry<String, String> entry : urlTemplates.entrySet()) {
      String sourceName = entry.getKey();
      String urlTemplate = entry.getValue();
      String extSourceUrl = strSubstitutor.replace(urlTemplate);
      urlTemplates.put(sourceName, extSourceUrl);
    }
    return new JSONObject(urlTemplates).toString();
  }
}
