package com.linkedin.thirdeye.detection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.dashboard.resources.v2.aggregation.AggregationLoader;
import com.linkedin.thirdeye.dashboard.resources.v2.aggregation.DefaultAggregationLoader;
import com.linkedin.thirdeye.dashboard.resources.v2.timeseries.DefaultTimeSeriesLoader;
import com.linkedin.thirdeye.dashboard.resources.v2.timeseries.TimeSeriesLoader;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DetectionConfigManager;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.wordnik.swagger.annotations.ApiParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/detection")
@Produces(MediaType.APPLICATION_JSON)
public class DetectionResource {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionResource.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final EventManager eventDAO;
  private final MergedAnomalyResultManager anomalyDAO;
  private final DetectionPipelineLoader loader;
  private final DataProvider provider;
  private final DetectionConfigManager configDAO;

  public DetectionResource() {

    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.eventDAO = DAORegistry.getInstance().getEventDAO();
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.configDAO = DAORegistry.getInstance().getDetectionConfigManager();

    TimeSeriesLoader timeseriesLoader =
        new DefaultTimeSeriesLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache());

    AggregationLoader aggregationLoader =
        new DefaultAggregationLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());

    this.loader = new DetectionPipelineLoader();

    this.provider =
        new DefaultDataProvider(metricDAO, eventDAO, anomalyDAO, timeseriesLoader, aggregationLoader, loader);
  }

  @POST
  @Path("/preview")
  public Response detectionPreview(@QueryParam("start") long start, @QueryParam("end") long end,
      @ApiParam("jsonPayload") String jsonPayload) throws Exception {
    if (jsonPayload == null) {
      throw new IllegalArgumentException("Empty Json Payload");
    }
    DetectionConfigDTO config = OBJECT_MAPPER.readValue(jsonPayload, DetectionConfigDTO.class);

    DetectionPipeline pipeline = this.loader.from(this.provider, config, start, end);
    DetectionPipelineResult result = pipeline.run();

    return Response.ok(result.getAnomalies()).build();
  }

  @POST
  @Path("/replay")
  public Response detectionReplay(@QueryParam("start") long start, @QueryParam("end") long end,
      @QueryParam("configId") long configId) throws Exception {

    DetectionConfigDTO config = this.configDAO.findById(configId);
    if (config == null) {
      throw new IllegalArgumentException(String.format("Cannot find config %d", configId));
    }

    DetectionPipeline pipeline = this.loader.from(this.provider, config, start, end);
    DetectionPipelineResult result = pipeline.run();

    for (MergedAnomalyResultDTO mergedAnomalyResultDTO : result.getAnomalies()) {
      this.anomalyDAO.update(mergedAnomalyResultDTO);
    }

    return Response.ok(result.getAnomalies()).build();
  }
}
