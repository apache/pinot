/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.api.detection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import io.dropwizard.auth.Auth;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants;
import org.apache.pinot.thirdeye.api.Constants;
import org.apache.pinot.thirdeye.api.user.dashboard.UserDashboardResource;
import org.apache.pinot.thirdeye.auth.ThirdEyePrincipal;
import org.apache.pinot.thirdeye.common.metric.MetricType;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.dashboard.resources.v2.anomalies.AnomalySearchFilter;
import org.apache.pinot.thirdeye.dashboard.resources.v2.anomalies.AnomalySearcher;
import org.apache.pinot.thirdeye.datalayer.bao.*;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.OnlineDetectionDataDTO;
import org.apache.pinot.thirdeye.datalayer.dto.TaskDTO;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.loader.AggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultAggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import org.apache.pinot.thirdeye.datasource.loader.TimeSeriesLoader;
import org.apache.pinot.thirdeye.detection.*;
import org.apache.pinot.thirdeye.detection.cache.builder.AnomaliesCacheBuilder;
import org.apache.pinot.thirdeye.detection.cache.builder.TimeSeriesCacheBuilder;
import org.apache.pinot.thirdeye.detection.validators.DetectionConfigValidator;
import org.apache.pinot.thirdeye.detection.yaml.DetectionConfigTuner;
import org.apache.pinot.thirdeye.detection.yaml.translator.DetectionConfigTranslator;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Api(tags = { Constants.DETECTION_TAG })
public class AnomalyDetectionResource {
  protected static final Logger LOG = LoggerFactory.getLogger(AnomalyDetectionResource.class);

  /* -------- Request/Response field -------- */
  private static final String DATA_FIELD = "data";
  private static final String COLUMNS_FIELD = "columns";
  private static final String ROWS_FIELD = "rows";
  private static final String DEFAULT_TIME_COLUMN = "date";
  private static final String DATASET_FIELD = "dataset-configuration";
  private static final String METRIC_FIELD = "metric-configuration";
  private static final String DETECTION_FIELD = "detection-configuration";
  private static final String ANOMALIES_FIELD = "anomalies";
  private static final String DATASET_CONFIG_TIME_COLUMN = "timeColumn";
  private static final String DATASET_CONFIG_TIME_UNIT = "timeUnit";
  private static final String DATASET_CONFIG_TIME_DURATION = "timeDuration";
  private static final String DATASET_CONFIG_TIME_FORMAT = "timeFormat";
  private static final String DATASET_CONFIG_TIME_ZONE = "timezone";
  private static final String METRIC_CONFIG_DATA_TYPE = "datatype";
  private static final String METRIC_CONFIG_METRIC_COLUMN = "metricColumn";

  /* -------- Others -------- */
  private static final String ONLINE_DATASOURCE = "OnlineThirdEyeDataSource";
  private static final String DEFAULT_DETECTION_NAME = "online_detection";
  private static final String DEFAULT_DATASET_NAME = "online_dataset";
  private static final String DEFAULT_METRIC_NAME = "online_metric";
  private static final String TEMPLATE_DETECTION_PATH = "detection-config-template.yml";
  private static final long POLLING_SLEEP_TIME = 5L;
  private static final long POLLING_TIMEOUT = 60 * 10L;
  private static final long MAX_ONLINE_PAYLOAD_SIZE = 10 * 1024 * 1024L;
  private static final int ANOMALIES_LIMIT = 500;

  private final DetectionConfigManager detectionConfigDAO;
  private final DataProvider provider;
  private final MetricConfigManager metricConfigDAO;
  private final DatasetConfigManager datasetConfigDAO;
  private final EventManager eventDAO;
  private final MergedAnomalyResultManager anomalyDAO;
  private final EvaluationManager evaluationDAO;
  private final TaskManager taskDAO;
  private final OnlineDetectionDataManager onlineDetectionDataDAO;
  private final DetectionPipelineLoader loader;
  private final DetectionConfigValidator detectionValidator;
  private final AnomalySearcher anomalySearcher;
  private final ObjectMapper objectMapper;
  private final Yaml yaml;

  @Inject
  public AnomalyDetectionResource(UserDashboardResource userDashboardResource) {
    this.detectionConfigDAO = DAORegistry.getInstance().getDetectionConfigManager();
    this.metricConfigDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetConfigDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.eventDAO = DAORegistry.getInstance().getEventDAO();
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.taskDAO = DAORegistry.getInstance().getTaskDAO();
    this.evaluationDAO = DAORegistry.getInstance().getEvaluationManager();
    this.onlineDetectionDataDAO = DAORegistry.getInstance().getOnlineDetectionDataManager();
    this.objectMapper = new ObjectMapper();

    TimeSeriesLoader timeseriesLoader =
        new DefaultTimeSeriesLoader(metricConfigDAO, datasetConfigDAO,
            ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getTimeSeriesCache());

    AggregationLoader aggregationLoader =
        new DefaultAggregationLoader(metricConfigDAO, datasetConfigDAO,
            ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());

    this.loader = new DetectionPipelineLoader();

    this.provider = new DefaultDataProvider(metricConfigDAO, datasetConfigDAO, eventDAO, anomalyDAO,
        evaluationDAO, timeseriesLoader, aggregationLoader, loader,
        TimeSeriesCacheBuilder.getInstance(), AnomaliesCacheBuilder.getInstance());
    this.detectionValidator = new DetectionConfigValidator(this.provider);
    this.anomalySearcher = new AnomalySearcher();

    DumperOptions options = new DumperOptions();
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
    options.setPrettyFlow(true);
    this.yaml = new Yaml(options);
  }

  /**
   * Run an online anomaly detection service synchronously. It will run anomaly detection using
   * default configs for detection, metric, dataset
   *
   * @param start     detection window start time
   * @param end       detection window end time
   * @param payload   payload in request including online data
   * @param principal user who sent this request. It's used to separate different config names
   * @return a message containing the detected anomalies and the detection config used
   */
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation("Request an anomaly detection online task")
  public Response onlineApi(
          @QueryParam("start") Long start,
          @QueryParam("end") Long end,
          @QueryParam("data-id") Long dataId,
          @ApiParam("jsonPayload") String payload,
          @Auth ThirdEyePrincipal principal) {
    DatasetConfigDTO datasetConfigDTO = null;
    MetricConfigDTO metricConfigDTO = null;
    DetectionConfigDTO detectionConfigDTO;
    TaskDTO taskDTO;
    Map<String, Object> anomalies;
    Response.Status responseStatus;
    Map<String, String> responseMessage = new HashMap<>();
    String nameSuffix = generateSuffix(principal);
    boolean dataRegistered = dataId != null;

    // TODO: refactor code to resolve request configurations in one place (e.g. default/customized config names)

    try {
      Preconditions.checkNotNull(start, "Detection start time is not provided");
      Preconditions.checkNotNull(end, "Detection end time is not provided");

      if (payload.getBytes().length > MAX_ONLINE_PAYLOAD_SIZE) {
        responseStatus = Response.Status.BAD_REQUEST;
        responseMessage.put("message", "Payload too large");
        return Response.status(responseStatus).entity(responseMessage).build();
      }

      JsonNode payloadNode = objectMapper.readTree(payload);

      Preconditions.checkArgument(validateOnlineRequestPayload(payloadNode, dataRegistered),
          "Invalid request payload");

      if (!dataRegistered) {
        // Create & save dataset
        datasetConfigDTO = generateDatasetConfig(payloadNode, nameSuffix);

        // Create & save metric
        metricConfigDTO = generateMetricConfig(payloadNode, nameSuffix);

        // Save online data
        saveOnlineDetectionData(payloadNode, datasetConfigDTO, metricConfigDTO);
      } else {
        // Data is already registered and dataset/metric config should also have been created
        OnlineDetectionDataDTO onlineDetectionDataDTO = onlineDetectionDataDAO.findById(dataId);
        Preconditions.checkNotNull(onlineDetectionDataDTO, "Data not found: " + dataId);

        String datasetName = onlineDetectionDataDTO.getDataset();
        String metricName = onlineDetectionDataDTO.getMetric();

        datasetConfigDTO = datasetConfigDAO.findByDataset(datasetName);
        metricConfigDTO = metricConfigDAO.findByMetricAndDataset(metricName, datasetName);

        Preconditions.checkNotNull(datasetConfigDTO,
            "Dataset not found for data id: " + dataId);

        Preconditions.checkNotNull(metricConfigDTO,
            "Metric not found for data id: " + dataId);
      }

      // Create & save detection
      detectionConfigDTO =
          generateDetectionConfig(payloadNode, nameSuffix, datasetConfigDTO, metricConfigDTO, start,
              end);

      // Create & save task
      taskDTO = generateTaskConfig(detectionConfigDTO, start, end, nameSuffix);

      // Polling task status
      TaskDTO polledTaskDTO = pollingTask(taskDTO.getId());

      // Task failure
      if (polledTaskDTO.getStatus() != TaskConstants.TaskStatus.COMPLETED) {
        LOG.warn("Task is not completed after polling: " + polledTaskDTO);

        responseStatus = Response.Status.INTERNAL_SERVER_ERROR;

        switch (polledTaskDTO.getStatus()) {
        case FAILED:
          responseStatus = Response.Status.INTERNAL_SERVER_ERROR;
          responseMessage.put("message", "Failed to execute anomaly detection task.");
          break;
        case TIMEOUT:
          responseStatus = Response.Status.REQUEST_TIMEOUT;
          responseMessage.put("message", "Anomaly detection task timeout.");
        default:
          LOG.error("Error task status after polling: " + polledTaskDTO.getStatus());
          responseMessage.put("message", "unknown task status.");
          break;
        }

        responseMessage.put("more-info", "Error = " + polledTaskDTO.getMessage());

        // Send response
        return Response.status(responseStatus).entity(responseMessage).build();
      }

      // Task success
      // Retrieve task result
      anomalies = getAnomalies(start, end, metricConfigDTO.getName(), datasetConfigDTO.getName());

      // Build success response
      JsonNode anomalyNode = objectMapper.convertValue(anomalies, JsonNode.class);
      ObjectNode responseNode = objectMapper.createObjectNode();
      responseNode.set(ANOMALIES_FIELD, anomalyNode);
      responseNode.set(DETECTION_FIELD, objectMapper.convertValue(detectionConfigDTO.getYaml(), JsonNode.class));
      responseNode.set(DATASET_FIELD, objectMapper.convertValue(translateDatasetToYaml(datasetConfigDTO), JsonNode.class));
      responseNode.set(METRIC_FIELD, objectMapper.convertValue(translateMetricToYaml(metricConfigDTO), JsonNode.class));

      responseStatus = Response.Status.OK;
      return Response.status(responseStatus).entity(objectMapper.writeValueAsString(responseNode))
          .build();
    } catch (JsonProcessingException e) {
      LOG.error("Error: {}", e.getMessage());
      responseStatus = Response.Status.BAD_REQUEST;
      responseMessage.put("message", "Invalid request payload");
      processException(e, responseMessage);
      return Response.status(responseStatus).entity(responseMessage).build();
    } catch (Exception e) {
      LOG.error("Error: {}", e.getMessage());
      responseStatus = Response.Status.INTERNAL_SERVER_ERROR;
      responseMessage.put("message", "Failed executing anomaly detection service.");
      processException(e, responseMessage);
      return Response.status(responseStatus).entity(responseMessage).build();
    } finally {
      cleanStates(metricConfigDTO, datasetConfigDTO, dataRegistered);
    }
  }

  boolean validateOnlineRequestPayload(JsonNode payloadNode, boolean dataRegistered) {
    if (!payloadNode.has(DATA_FIELD))
      return dataRegistered;

    JsonNode dataNode = payloadNode.get(DATA_FIELD);
    if (!dataNode.has(COLUMNS_FIELD) || !dataNode.has(ROWS_FIELD))
      return false;

    JsonNode columnsNode = dataNode.get(COLUMNS_FIELD);
    if (!columnsNode.isArray()) return false;

    return true;
  }

  boolean validateOnlineDetectionData(JsonNode dataNode, String timeColumnName, String metricColumnName) {
    // Check if time & metric columns exist in adhoc data
    ArrayNode columnsNode = dataNode.withArray(COLUMNS_FIELD);
    int[] colIndices = findTimeAndMetricColumns(columnsNode,
        timeColumnName, metricColumnName);
    int timeColIdx = colIndices[0];
    int metricColIdx = colIndices[1];
    return metricColIdx>=0 && timeColIdx>=0;
  }

  DatasetConfigDTO generateDatasetConfig(JsonNode payloadNode, String suffix) {
    DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();

    // Default configuration
    datasetConfigDTO.setDataset(DEFAULT_DATASET_NAME + suffix);
    datasetConfigDTO.setDimensions(Collections.unmodifiableList(new ArrayList<>()));
    datasetConfigDTO.setTimeColumn(DEFAULT_TIME_COLUMN);
    datasetConfigDTO.setTimeDuration(1);
    datasetConfigDTO.setTimeUnit(TimeUnit.DAYS);
    datasetConfigDTO.setTimeFormat("SIMPLE_DATE_FORMAT:yyyyMMdd");
    datasetConfigDTO.setDataSource(ONLINE_DATASOURCE);

    // Customized configuration
    if (payloadNode.has(DATASET_FIELD)) {
      updateDatasetCustomFields(payloadNode, datasetConfigDTO);
    }

    datasetConfigDAO.save(datasetConfigDTO);
    LOG.info("Created dataset with config {}", datasetConfigDTO);

    return datasetConfigDTO;
  }

  MetricConfigDTO generateMetricConfig(JsonNode payloadNode, String suffix) {
    MetricConfigDTO metricConfigDTO = new MetricConfigDTO();

    // Default configuration
    metricConfigDTO.setName(DEFAULT_METRIC_NAME);
    metricConfigDTO.setDataset(DEFAULT_DATASET_NAME + suffix);
    metricConfigDTO.setAlias(ThirdEyeUtils
        .constructMetricAlias(DEFAULT_DATASET_NAME + suffix,
            DEFAULT_METRIC_NAME));
    metricConfigDTO.setDatatype(MetricType.DOUBLE);
    metricConfigDTO.setDefaultAggFunction(MetricAggFunction.SUM);
    metricConfigDTO.setActive(true);

    // Customized configuration
    if (payloadNode.has(METRIC_FIELD)) {
      updateMetricCustomFields(payloadNode, metricConfigDTO);
    }

    metricConfigDAO.save(metricConfigDTO);
    LOG.info("Created metric with config {}", metricConfigDTO);

    return metricConfigDTO;
  }

  DetectionConfigDTO generateDetectionConfig(JsonNode payloadNode, String suffix,
      DatasetConfigDTO datasetConfigDTO, MetricConfigDTO metricConfigDTO, long start, long end) {
    DetectionConfigDTO detectionConfigDTO;
    Map<String, Object> detectionYaml;
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    if (payloadNode.has(DETECTION_FIELD)) {
      // Customized configuration: retrieve config from user request
      detectionYaml = ConfigUtils.getMap(yaml.load(payloadNode.get(DETECTION_FIELD).textValue()));
    } else {
      // Default configuration: retrieve the template from disk
      detectionYaml =
          ConfigUtils.getMap(yaml.load(classLoader.getResourceAsStream(TEMPLATE_DETECTION_PATH)));
    }

    // Do not support customized detection name as it is not a common use case
    detectionYaml.put("detectionName", DEFAULT_DETECTION_NAME + suffix);
    detectionYaml.put("dataset", datasetConfigDTO.getName());
    detectionYaml.put("metric", metricConfigDTO.getName());

    detectionConfigDTO =
        new DetectionConfigTranslator(this.yaml.dump(detectionYaml), this.provider).translate();
    detectionConfigDTO.setCron("0 0 0 1 1 ? 2200"); // Never scheduled

    // Tune the detection config - Passes the raw yaml params & injects tuned params
    DetectionConfigTuner detectionTuner = new DetectionConfigTuner(detectionConfigDTO, provider);
    detectionConfigDTO = detectionTuner.tune(start, end);

    // Validate the detection config
    detectionValidator.validateConfig(detectionConfigDTO);

    // Online detection will not save detect config into DB

    LOG.info("Created detection with config {}", detectionConfigDTO);

    return detectionConfigDTO;
  }

  TaskDTO generateTaskConfig(DetectionConfigDTO detectionConfigDTO,
        long start, long end, String nameSuffix)
      throws JsonProcessingException {
    TaskDTO taskDTO = new TaskDTO();
    taskDTO.setJobName(
        TaskConstants.TaskType.DETECTION_ONLINE.toString() + nameSuffix);
    taskDTO.setStatus(TaskConstants.TaskStatus.WAITING);
    taskDTO.setTaskType(TaskConstants.TaskType.DETECTION_ONLINE);
    DetectionPipelineTaskInfo taskInfo =
        new DetectionPipelineTaskInfo(-1L, start, end);
    taskInfo.setOnline(true);

    // Store the detection config into online task info
    taskDAO.populateDetectionConfig(detectionConfigDTO, taskInfo);

    String taskInfoJson = objectMapper.writeValueAsString(taskInfo);
    taskDTO.setTaskInfo(taskInfoJson);

    taskDAO.save(taskDTO);
    LOG.info("Created task: {}", taskDTO);

    return taskDTO;
  }

  OnlineDetectionDataDTO saveOnlineDetectionData(JsonNode payloadNode,
      DatasetConfigDTO datasetConfigDTO, MetricConfigDTO metricConfigDTO)
        throws JsonProcessingException {
    JsonNode dataNode = payloadNode.get(DATA_FIELD);
    String timeColumnName = datasetConfigDTO.getTimeColumn();
    String datasetName = datasetConfigDTO.getDataset();
    String metricName = metricConfigDTO.getName();

    // Check if time & metric columns exist in adhoc data
    Preconditions.checkArgument(validateOnlineDetectionData(dataNode, timeColumnName, metricName),
        String.format("metric: %s or time: %s not found in adhoc data.",
            metricName, timeColumnName));

    // Save online data
    OnlineDetectionDataDTO onlineDetectionDataDTO = new OnlineDetectionDataDTO();
    onlineDetectionDataDTO.setDataset(datasetName);
    onlineDetectionDataDTO.setMetric(metricName);
    onlineDetectionDataDTO.setOnlineDetectionData(this.objectMapper.writeValueAsString(dataNode));

    onlineDetectionDataDAO.save(onlineDetectionDataDTO);

    LOG.info("Saved online data with dataset: {} and metric: {}",
        onlineDetectionDataDTO.getDataset(), onlineDetectionDataDTO.getMetric());

    return onlineDetectionDataDTO;
  }

  private TaskDTO pollingTask(long taskId) {
    long startTime = System.currentTimeMillis();
    TaskDTO taskDTO;

    // Add timeout mechanism in case anything external goes wrong
    while (true) {
      taskDTO = taskDAO.findById(taskId);

      LOG.info("Polling task : " + taskDTO);

      TaskConstants.TaskStatus taskStatus = taskDTO.getStatus();
      if (!taskStatus.equals(TaskConstants.TaskStatus.WAITING) &&
              !taskStatus.equals(TaskConstants.TaskStatus.RUNNING)) {
        LOG.info("Polling finished ({}ms). Task status: {}",
            System.currentTimeMillis() - startTime, taskStatus);
        break;
      }

      try {
        TimeUnit.SECONDS.sleep(POLLING_SLEEP_TIME);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted during polling sleep");
        break;
      }

      if ( (System.currentTimeMillis() - startTime) > TimeUnit.SECONDS.toMillis(POLLING_TIMEOUT)) {
        LOG.warn("Polling timeout. Mark task as FAILED");
        taskDTO.setStatus(TaskConstants.TaskStatus.FAILED);
        break;
      }
    }

    return taskDTO;
  }

  private Map<String, Object> getAnomalies(long start, long end, String metric, String dataset) {
    AnomalySearchFilter searchFilter =
        new AnomalySearchFilter(start, end, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
            Collections.singletonList(metric), Collections.singletonList(dataset), Collections.emptyList());

    Map<String, Object> anomalies = this.anomalySearcher.search(searchFilter, ANOMALIES_LIMIT, 0);

    LOG.info("Successfully returned " + anomalies.get("count") + " anomalies.");
    return anomalies;
  }

  private void cleanStates(MetricConfigDTO metricConfigDTO, DatasetConfigDTO datasetConfigDTO,
      boolean dataRegistered) {
    // Registered online data should not be cleaned
    if (dataRegistered) return;

    if (datasetConfigDTO != null) {
      int onlineDetectionDataCnt = onlineDetectionDataDAO
          .deleteByPredicate(Predicate.EQ("dataset", datasetConfigDTO.getName()));
      LOG.info("Deleted {} online data with dataset {}",
          onlineDetectionDataCnt, datasetConfigDTO.getName());
    }

    if (metricConfigDTO != null) {
      int onlineDetectionDataCnt = onlineDetectionDataDAO
          .deleteByPredicate(Predicate.EQ("metric", metricConfigDTO.getName()));
      LOG.info("Deleted {} online data with metric {}",
          onlineDetectionDataCnt, metricConfigDTO.getName());
    }
  }

  private int[] findTimeAndMetricColumns(JsonNode node, String timeColName, String metricColName) {
    int metricColIdx = -1, timeColIdx = -1;
    if (node.isArray()) {
      for (int colIdx = 0; colIdx < node.size(); colIdx++) {
        if (node.get(colIdx).textValue().equals(timeColName)) {
          timeColIdx = colIdx;
        }

        if (node.get(colIdx).textValue().equals(metricColName)) {
          metricColIdx = colIdx;
        }
      }
    }
    return new int[]{timeColIdx, metricColIdx};
  }

  String generateSuffix(ThirdEyePrincipal principal) {
    // Suffix format: _<user_name>_<uuid>
    return "_" + principal.getName() + "_" + UUID.randomUUID().toString();
  }

  private void updateDatasetCustomFields(JsonNode payloadNode, DatasetConfigDTO datasetConfigDTO) {
    Map<String, Object> datasetYaml =
        ConfigUtils.getMap(yaml.load(payloadNode.get(DATASET_FIELD).textValue()));

    if (datasetYaml.containsKey(DATASET_CONFIG_TIME_COLUMN)) {
      datasetConfigDTO.setTimeColumn((String) datasetYaml.get(DATASET_CONFIG_TIME_COLUMN));
    }
    if (datasetYaml.containsKey(DATASET_CONFIG_TIME_UNIT)) {
      datasetConfigDTO
          .setTimeUnit(TimeUnit.valueOf((String) datasetYaml.get(DATASET_CONFIG_TIME_UNIT)));
    }
    if (datasetYaml.containsKey(DATASET_CONFIG_TIME_DURATION)) {
      datasetConfigDTO.setTimeDuration((Integer) datasetYaml.get(DATASET_CONFIG_TIME_DURATION));
    }
    if (datasetYaml.containsKey(DATASET_CONFIG_TIME_FORMAT)) {
      datasetConfigDTO.setTimeFormat((String) datasetYaml.get(DATASET_CONFIG_TIME_FORMAT));
    }
    if (datasetYaml.containsKey(DATASET_CONFIG_TIME_ZONE)) {
      datasetConfigDTO.setTimezone((String) datasetYaml.get(DATASET_CONFIG_TIME_ZONE));
    }
  }

  private void updateMetricCustomFields(JsonNode payloadNode, MetricConfigDTO metricConfigDTO) {
    Map<String, Object> metricYaml =
        ConfigUtils.getMap(yaml.load(payloadNode.get(METRIC_FIELD).textValue()));

    // Customized metric name
    if (metricYaml.containsKey(METRIC_CONFIG_METRIC_COLUMN)) {
      metricConfigDTO.setName((String) metricYaml.get(METRIC_CONFIG_METRIC_COLUMN));
    }

    if (metricYaml.containsKey(METRIC_CONFIG_DATA_TYPE)) {
      metricConfigDTO
          .setDatatype(MetricType.valueOf((String) metricYaml.get(METRIC_CONFIG_DATA_TYPE)));
    }
  }

  /**
   * Register the online detection ad-hoc data with optional customized dataset and metric configurations.
   *
   * @param payload     payload in request including online data
   * @param principal user who sent this request. It's used to separate different config names
   * @return a message containing an ID referring to the ad-hoc data. This ID can be used to perform
   * CRUD operations on the registered data.
   */
  @POST
  @Path("/data")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation("Register ad hoc data used in online service and created dataset/metric configs")
  public Response createOnlineData(@ApiParam("jsonPayload") String payload,
      @Auth ThirdEyePrincipal principal) {
    Response.Status responseStatus;
    Map<String, String> responseMessage = new HashMap<>();
    ObjectMapper objectMapper = new ObjectMapper();
    DatasetConfigDTO datasetConfigDTO;
    MetricConfigDTO metricConfigDTO;
    String suffix = generateSuffix(principal);

    try {
      JsonNode payloadNode = objectMapper.readTree(payload);

      if (!validateOnlineRequestPayload(payloadNode, false)) {
        responseStatus = Response.Status.BAD_REQUEST;
        responseMessage.put("message", "Invalid request payload");
        return Response.status(responseStatus).entity(responseMessage).build();
      }

      // Create & save dataset
      datasetConfigDTO = generateDatasetConfig(payloadNode, suffix);

      // Create & save metric
      metricConfigDTO = generateMetricConfig(payloadNode, suffix);

      // Save online data
      OnlineDetectionDataDTO onlineDetectionDataDTO =
          saveOnlineDetectionData(payloadNode, datasetConfigDTO, metricConfigDTO);

      responseMessage.put("data-id", "" + onlineDetectionDataDTO.getId());

      return Response.ok().entity(responseMessage).build();
    } catch (JsonProcessingException e) {
      LOG.error("Error: {}", e.getMessage());
      responseStatus = Response.Status.BAD_REQUEST;
      responseMessage.put("message", "Invalid request payload");
      processException(e, responseMessage);
      return Response.status(responseStatus).entity(responseMessage).build();
    } catch (Exception e) {
      LOG.error("Error: {}", e.getMessage());
      responseStatus = Response.Status.INTERNAL_SERVER_ERROR;
      responseMessage.put("message", "Failed to register data.");
      processException(e, responseMessage);
      return Response.status(responseStatus).entity(responseMessage).build();
    }
  }

  /**
   * Update the online detection ad-hoc data with optional customized dataset and metric configurations.
   *
   * @param payload payload in request including online data
   * @param dataId the ID referring to the online detection ad-hoc data
   * @param principal user who sent this request. It's used to separate different config names
   * @return a message containing an ID referring to the ad-hoc data. This ID can be used to perform
   * CRUD operations on the registered data.
   */
  @PUT
  @Path("/data/{data-id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation("Update ad hoc data used in online service and corresponding dataset/metric configs")
  public Response updateOnlineData(
      @ApiParam("jsonPayload") String payload,
      @PathParam("data-id") Long dataId,
      @Auth ThirdEyePrincipal principal) {
    Preconditions.checkNotNull(dataId, "online detection data id is null");

    Response.Status responseStatus;
    Map<String, String> responseMessage = new HashMap<>();
    DatasetConfigDTO datasetConfigDTO;
    MetricConfigDTO metricConfigDTO;

    try {
      JsonNode payloadNode = objectMapper.readTree(payload);

      Preconditions.checkArgument(payloadNode.has(DATA_FIELD), "Update should have new data");

      OnlineDetectionDataDTO onlineDetectionDataDTO = onlineDetectionDataDAO.findById(dataId);
      Preconditions.checkNotNull(onlineDetectionDataDTO,
          "Online detection data not found " + dataId);

      LOG.info("Find online detection data with dataset {} and metric {}",
          onlineDetectionDataDTO.getDataset(), onlineDetectionDataDTO.getMetric());

      // Find existing dataset config
      datasetConfigDTO = datasetConfigDAO.findByDataset(onlineDetectionDataDTO.getDataset());
      Preconditions.checkNotNull(datasetConfigDTO,
          "No corresponding dataset found for online data " + dataId);

      // Find existing metric config
      metricConfigDTO = metricConfigDAO.findByMetricAndDataset(
          onlineDetectionDataDTO.getMetric(), onlineDetectionDataDTO.getDataset());
      Preconditions.checkNotNull(metricConfigDTO,
          "No corresponding metric found for online data " + dataId);

      // Update dataset config
      onlineDetectionDataDTO = doUpdateOnlineData(payloadNode, datasetConfigDTO,
          metricConfigDTO, onlineDetectionDataDTO);

      responseMessage.put("data-id", "" + onlineDetectionDataDTO.getId());
      responseStatus = Response.Status.OK;

      return Response.status(responseStatus).entity(responseMessage).build();
    } catch (JsonProcessingException e) {
      LOG.error("Error: {}", e.getMessage());
      responseStatus = Response.Status.BAD_REQUEST;
      responseMessage.put("message", "Invalid request payload");
      processException(e, responseMessage);
      return Response.status(responseStatus).entity(responseMessage).build();
    } catch (Exception e) {
      LOG.error("Error: {}", e.getMessage());
      responseStatus = Response.Status.INTERNAL_SERVER_ERROR;
      responseMessage.put("message", "Failed executing anomaly detection service.");
      processException(e, responseMessage);
      return Response.status(responseStatus).entity(responseMessage).build();
    }
  }

  OnlineDetectionDataDTO doUpdateOnlineData(JsonNode payloadNode,
      DatasetConfigDTO datasetConfigDTO, MetricConfigDTO metricConfigDTO,
      OnlineDetectionDataDTO onlineDetectionDataDTO) throws JsonProcessingException {
    if (payloadNode.has(DATASET_FIELD)) {
      updateDatasetCustomFields(payloadNode, datasetConfigDTO);
      LOG.info("Update dataset config to {}", datasetConfigDTO);
    }

    // Update metric config
    if (payloadNode.has(METRIC_FIELD)) {
      updateMetricCustomFields(payloadNode, metricConfigDTO);
      onlineDetectionDataDTO.setMetric(metricConfigDTO.getName());
      LOG.info("Update metric config to {}", metricConfigDTO);
    }

    JsonNode dataNode = payloadNode.get(DATA_FIELD);

    Preconditions.checkArgument(
        validateOnlineDetectionData(dataNode, datasetConfigDTO.getTimeColumn(), metricConfigDTO.getName()),
        String.format("metric: %s or time: %s not found in adhoc data.",
            metricConfigDTO.getName(), datasetConfigDTO.getTimeColumn()));

    onlineDetectionDataDTO.setOnlineDetectionData(this.objectMapper.writeValueAsString(dataNode));

    datasetConfigDAO.update(datasetConfigDTO);
    metricConfigDAO.update(metricConfigDTO);
    onlineDetectionDataDAO.update(onlineDetectionDataDTO);
    LOG.info("Save updated online detection data for {}", onlineDetectionDataDTO.getId());

    return onlineDetectionDataDTO;
  }

  /**
   * Delete the online detection ad-hoc data with optional customized dataset and metric configurations.
   *
   * @param dataId the ID referring to the online detection ad-hoc data
   * @return a message containing an ID referring to the ad-hoc data. This ID can be used to perform
   * CRUD operations on the registered data.
   */
  @DELETE
  @Path("/data/{data-id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation("Delete ad hoc data used in online service and corresponding dataset/metric configs")
  public Response deleteOnlineData(@PathParam("data-id") Long dataId) {
    Preconditions.checkNotNull(dataId, "online detection data id is null");

    Response.Status responseStatus;
    Map<String, String> responseMessage = new HashMap<>();

    try {
      long cnt = onlineDetectionDataDAO.deleteById(dataId);
      responseStatus = Response.Status.OK;
      LOG.info("Deleted {} online detection data by id {}", cnt, dataId);
      responseMessage.put("data-id", ""+dataId);
      return Response.status(responseStatus).entity(responseMessage).build();
    } catch (Exception e) {
      LOG.error("Error: {}", e.getMessage());
      responseStatus = Response.Status.INTERNAL_SERVER_ERROR;
      responseMessage.put("message", "Failed executing anomaly detection service.");
      processException(e, responseMessage);
      return Response.status(responseStatus).entity(responseMessage).build();
    }
  }

  /**
   * Query the online detection ad-hoc data with optional customized dataset and metric configurations.
   *
   * @param dataId the ID referring to the online detection ad-hoc data
   * @return a message containing the online ad-hoc data and corresponding dataset and metric configurations.
   */
  @GET
  @Path("/data/{data-id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation("Get ad hoc data used in online service and corresponding dataset/metric configs")
  public Response getOnlineData(@PathParam("data-id") Long dataId) {
    Preconditions.checkNotNull(dataId, "online detection data id is null");

    Response.Status responseStatus;
    Map<String, String> responseMessage = new HashMap<>();

    try {
      OnlineDetectionDataDTO onlineDetectionDataDTO = onlineDetectionDataDAO.findById(dataId);
      Preconditions.checkNotNull(onlineDetectionDataDTO,
          "Online detection data not found " + dataId);

      LOG.info("Find online detection data with dataset {} and metric {}",
          onlineDetectionDataDTO.getDataset(), onlineDetectionDataDTO.getMetric());

      DatasetConfigDTO datasetConfigDTO = datasetConfigDAO.findByDataset(onlineDetectionDataDTO.getDataset());
      Preconditions.checkNotNull(datasetConfigDTO,
          "No corresponding dataset found for online data " + dataId);

      MetricConfigDTO metricConfigDTO = metricConfigDAO.findByMetricAndDataset(
          onlineDetectionDataDTO.getMetric(), onlineDetectionDataDTO.getDataset());
      Preconditions.checkNotNull(metricConfigDTO,
          "No corresponding metric found for online data " + dataId);

      ObjectNode responseNode = objectMapper.createObjectNode();
      responseNode.set(DATA_FIELD, objectMapper.readTree(onlineDetectionDataDTO.getOnlineDetectionData()));
      responseNode.set(DATASET_FIELD, objectMapper.convertValue(translateDatasetToYaml(datasetConfigDTO), JsonNode.class));
      responseNode.set(METRIC_FIELD, objectMapper.convertValue(translateMetricToYaml(metricConfigDTO), JsonNode.class));

      responseStatus = Response.Status.OK;

      return Response.status(responseStatus).entity(responseNode).build();
    } catch (Exception e) {
      LOG.error("Error: {}", e.getMessage());
      responseStatus = Response.Status.INTERNAL_SERVER_ERROR;
      responseMessage.put("message", "Failed executing anomaly detection service.");
      processException(e, responseMessage);
      return Response.status(responseStatus).entity(responseMessage).build();
    }
  }

  String translateMetricToYaml(MetricConfigDTO metricConfigDTO) {
    Map<String, String> configurations = new HashMap<>();
    configurations.put(METRIC_CONFIG_DATA_TYPE, metricConfigDTO.getDatatype().toString());
    configurations.put(METRIC_CONFIG_METRIC_COLUMN, metricConfigDTO.getName());

    return yaml.dump(configurations);
  }

  String translateDatasetToYaml(DatasetConfigDTO datasetConfigDTO) {
    Map<String, String> configurations = new HashMap<>();
    configurations.put(DATASET_CONFIG_TIME_COLUMN, datasetConfigDTO.getTimeColumn());
    configurations.put(DATASET_CONFIG_TIME_UNIT, datasetConfigDTO.getTimeUnit().toString());
    configurations.put(DATASET_CONFIG_TIME_DURATION, datasetConfigDTO.getTimeDuration().toString());
    configurations.put(DATASET_CONFIG_TIME_FORMAT, datasetConfigDTO.getTimeFormat());
    configurations.put(DATASET_CONFIG_TIME_ZONE, datasetConfigDTO.getTimezone());

    return yaml.dump(configurations);
  }

  /**
   * Given a detection config name, run a anomaly detection task using this detection config
   * asynchronously. It will return a task ID for query the task status later.
   *
   * @param start         detection window start time
   * @param end           detection window end time
   * @param detectionName the name of the detection config already existing in TE database
   * @return a message containing the ID of the anomaly detection task and HATEOAS links
   */
  @POST
  @Path("/tasks")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  @ApiOperation("Submit an anomaly detection task")
  public Response taskSubmitApi(
      @QueryParam("start") long start,
      @QueryParam("end") long end,
      @QueryParam("detection-name") String detectionName) {
    long ts = System.currentTimeMillis();
    Map<String, String> responseMessage = new HashMap<>();
    try {
      // Find detection by name
      List<DetectionConfigDTO> detectionConfigDTOS =
          detectionConfigDAO.findByPredicate(Predicate.EQ("name", detectionName));

      // Precondition check
      if (detectionConfigDTOS.isEmpty()) {
        LOG.warn("Detection config not found: {}", detectionName);
        responseMessage.put("message", "Detection config not found: " + detectionName);
        return Response.status(Response.Status.NOT_FOUND).entity(responseMessage).build();
      } else if (detectionConfigDTOS.size() > 1) {
        LOG.error("Duplicate detection configs: {}", detectionConfigDTOS);
        responseMessage.put("message", "Duplicate detection configs");
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(responseMessage)
            .build();
      }

      DetectionConfigDTO detectionConfigDTO = detectionConfigDTOS.get(0);

      LOG.info("Find detection config: {}", detectionConfigDTO);

      // Create task
      DetectionPipelineTaskInfo taskInfo =
          new DetectionPipelineTaskInfo(detectionConfigDTO.getId(), start, end);
      String taskInfoJson;
      TaskDTO taskDTO;
      long taskId;
      try {
        taskInfoJson = objectMapper.writeValueAsString(taskInfo);
        taskDTO = TaskUtils
            .buildTask(taskInfo.getConfigId(), taskInfoJson, TaskConstants.TaskType.DETECTION);
        taskId = taskDAO.save(taskDTO);
        LOG.info("Saved task: " + taskDTO + " into DB");
        LOG.info("Create task successful, used {} milliseconds", System.currentTimeMillis() - ts);
      } catch (JsonProcessingException e) {
        LOG.error("Exception when converting DetectionPipelineTaskInfo {} to jsonString", taskInfo,
            e);
        responseMessage.put("message", "Error while creating detection task");
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(responseMessage)
            .build();
      }

      // Build HATEOAS response
      ObjectNode responseJson = buildResponseJson(
          UriBuilder.fromPath("/anomaly-detection/tasks")
              .build().toString(), "POST");

      Response.Status responseStatus = Response.Status.CREATED;

      if (taskId < 0) {
        responseStatus = Response.Status.BAD_REQUEST;
        return Response.status(responseStatus).entity(responseJson).build();
      }

      responseJson.put("task-id", taskId);
      addLink(responseJson, "task-status",
          UriBuilder.fromPath("/anomaly-detection/task/{task-id}")
                .resolveTemplate("task-id", taskId)
                .build().toString(), "GET");

      return Response.status(responseStatus).entity(responseJson).build();
    } catch (IllegalArgumentException e) {
      LOG.error("Error: {}", e.getMessage());
      responseMessage.put("message", "Failed submitting anomaly detection task.");
      processException(e, responseMessage);
      return Response.status(Response.Status.BAD_REQUEST).entity(responseMessage).build();
    } catch (Exception e) {
      LOG.error("Error: {}", e.getMessage());
      responseMessage.put("message", "Failed submitting anomaly detection task.");
      processException(e, responseMessage);
      return Response.serverError().entity(responseMessage).build();
    }
  }

  private void processException(Throwable e, Map<String, String> responseMessage) {
    StringBuilder sb = new StringBuilder();
    // show more stack message to frontend for debugging
    getErrorMessage(0, 5, e, sb);
    responseMessage.put("more-info", "Error stack: " + sb.toString());
  }

  private void getErrorMessage(int curLevel, int totalLevel, Throwable e, StringBuilder sb) {
    if (curLevel <= totalLevel && e != null) {
      sb.append("==");
      if (e.getMessage() != null) {
        sb.append(e.getMessage());
      }
      getErrorMessage(curLevel + 1, totalLevel, e.getCause(), sb);
    }
  }

  /**
   * Given a anomaly detection task ID, return its current status.
   *
   * @param taskId the ID of the anomaly detection task to be queried
   * @return a message containing the status of the task and HATEOAS links
   */
  @GET
  @Path("/task/{task-id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  @ApiOperation("Query a task status")
  public Response taskStatusApi(@PathParam("task-id") long taskId) {
    Map<String, String> responseMessage = new HashMap<>();

    try {
      // Find task
      TaskDTO taskDTO = taskDAO.findById(taskId);

      // Precondition check
      LOG.info("Try to find task by ID: " + taskId);
      if (taskDTO == null) {
        LOG.warn("Task not found {}", taskId);
        responseMessage.put("message", "Task not found: " + taskId);
        return Response.status(Response.Status.NOT_FOUND).entity(responseMessage).build();
      }

      LOG.info("Found task" + taskDTO);

      // Check task status
      TaskConstants.TaskStatus taskStatus = taskDTO.getStatus();
      Response.Status responseStatus;
      if (taskStatus.equals(TaskConstants.TaskStatus.COMPLETED)) {
        responseStatus = Response.Status.SEE_OTHER;
      } else {
        responseStatus = Response.Status.ACCEPTED;
      }

      // Build HATEOAS response
      ObjectNode responseJson = buildResponseJson(
          UriBuilder.fromPath("/anomaly-detection/task/{task-id}")
              .resolveTemplate("task-id", taskId).build().toString(), "GET");

      responseJson.put("task-status", taskStatus.name());

      if (responseStatus.equals(Response.Status.SEE_OTHER)) {
        addLink(responseJson, "anomalies", "/userdashboard/anomalies", "GET");
      }

      return Response.status(responseStatus).entity(responseJson).build();
    } catch (Exception e) {
      LOG.error("Error: {}", e.getMessage());
      responseMessage.put("message", "Failed querying anomaly detection task.");
      processException(e, responseMessage);
      return Response.serverError().entity(responseMessage).build();
    }
  }

  /* ----------- HATEOAS Utilities --------------- */
  private ObjectNode buildResponseJson(String selfUri, String selfMethod) {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode rootNode = mapper.createObjectNode();

    ObjectNode linksNode = mapper.createObjectNode();
    ObjectNode selfNode = mapper.createObjectNode();

    rootNode.set("_links", linksNode);
    linksNode.set("self", selfNode);

    selfNode.put("href", selfUri);
    selfNode.put("method", selfMethod);

    return rootNode;
  }

  private void addLink(ObjectNode rootNode, String rel, String href, String method) {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode linkNode = mapper.createObjectNode();
    linkNode.put("href", href);
    linkNode.put("method", method);
    ((ObjectNode) rootNode.get("_links")).set(rel, linkNode);
  }
}
