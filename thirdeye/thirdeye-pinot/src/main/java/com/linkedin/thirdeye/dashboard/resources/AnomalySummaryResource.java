package com.linkedin.thirdeye.dashboard.resources;

import com.linkedin.thirdeye.anomaly.merge.AnomalyMergeConfig;
import com.linkedin.thirdeye.anomaly.merge.AnomalyTimeBasedSummarizer;
import com.linkedin.thirdeye.api.dto.GroupByKey;
import com.linkedin.thirdeye.api.dto.GroupByRow;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;

import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.StringUtils;

@Path("thirdeye/anomaly")
@Produces(MediaType.APPLICATION_JSON)
public class AnomalySummaryResource {

  private RawAnomalyResultManager resultDAO;
  private MergedAnomalyResultManager mergedResultDAO;

  public AnomalySummaryResource(RawAnomalyResultManager resultDAO,
      MergedAnomalyResultManager mergedResultDAO) {
    this.resultDAO = resultDAO;
    this.mergedResultDAO = mergedResultDAO;
  }

  @GET
  @Path("merged")
  public List<MergedAnomalyResultDTO> getMergedResults(@QueryParam("startTime") Long startTime,
      @QueryParam("endTime") Long endTime) {
    if(startTime == null) {
      startTime = 0l;
    }
    if(endTime == null) {
      endTime = System.currentTimeMillis();
    }
    List<MergedAnomalyResultDTO> mergedResults = mergedResultDAO.getAllByTime(startTime, endTime);
    return mergedResults;
  }

  @GET
  @Path("summary/function/{functionId}")
  public List<MergedAnomalyResultDTO> getSummaryForFunction(@PathParam("functionId") Long functionId,
      @QueryParam("dimensions") String dimensions) {
    return getAnomalySummaryForFunction(functionId, dimensions, new AnomalyMergeConfig());
  }

  @POST
  @Path("summary/function/{functionId}")
  public List<MergedAnomalyResultDTO> getAnomalySummaryForFunction(
      @PathParam("functionId") Long functionId, @QueryParam("dimensions") String dimensions,
      AnomalyMergeConfig mergeConfig) {
    if (mergeConfig == null) {
      mergeConfig = new AnomalyMergeConfig();
    }
    if (functionId == null) {
      throw new IllegalArgumentException("Function id can't be null");
    }

    List<RawAnomalyResultDTO> anomalies;
    if (!StringUtils.isEmpty(dimensions)) {
      anomalies = resultDAO.findAllByTimeFunctionIdAndDimensions(mergeConfig.getStartTime(),
          mergeConfig.getEndTime(), functionId, dimensions);
    } else {
      anomalies = resultDAO
          .findAllByTimeAndFunctionId(mergeConfig.getStartTime(), mergeConfig.getEndTime(),
              functionId);
    }
    return AnomalyTimeBasedSummarizer.mergeAnomalies(anomalies, mergeConfig.getMergeDuration(),
        mergeConfig.getSequentialAllowedGap());
  }

  @POST
  @Path("summary/groupBy")
  public List<GroupByRow<GroupByKey, Long>> getAnomalyResultsByMergeGroup(
      AnomalyMergeConfig mergeConfig) {
    if (mergeConfig == null) {
      mergeConfig = new AnomalyMergeConfig();
    }
    switch (mergeConfig.getMergeStrategy()) {
    case FUNCTION_DIMENSIONS:
      return resultDAO.getCountByFunctionDimensions(mergeConfig.getStartTime(), mergeConfig.getEndTime());
    case FUNCTION:
      return resultDAO.getCountByFunction(mergeConfig.getStartTime(), mergeConfig.getEndTime());
    default:
      throw new IllegalArgumentException("Unknown merge strategy : " + mergeConfig.getMergeStrategy());
    }
  }
}
