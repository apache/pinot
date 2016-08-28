package com.linkedin.thirdeye.dashboard.resources;

import com.linkedin.thirdeye.anomaly.merge.AnomalyMergeConfig;
import com.linkedin.thirdeye.anomaly.merge.AnomalyTimeBasedSummarizer;
import com.linkedin.thirdeye.api.dto.GroupByKey;
import com.linkedin.thirdeye.api.dto.GroupByRow;
import com.linkedin.thirdeye.db.dao.AnomalyMergedResultDAO;
import com.linkedin.thirdeye.db.entity.AnomalyMergedResult;
import com.linkedin.thirdeye.db.dao.AnomalyResultDAO;
import com.linkedin.thirdeye.db.entity.AnomalyResult;
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

  private AnomalyResultDAO resultDAO;
  private AnomalyMergedResultDAO mergedResultDAO;

  public AnomalySummaryResource(AnomalyResultDAO resultDAO,
      AnomalyMergedResultDAO mergedResultDAO) {
    this.resultDAO = resultDAO;
    this.mergedResultDAO = mergedResultDAO;
  }

  @GET
  @Path("merged")
  public List<AnomalyMergedResult> getMergedResults(@QueryParam("startTime") Long startTime,
      @QueryParam("endTime") Long endTime) {
    if(startTime == null) {
      startTime = 0l;
    }
    if(endTime == null) {
      endTime = System.currentTimeMillis();
    }
    List<AnomalyMergedResult> mergedResults = mergedResultDAO.getAllByTime(startTime, endTime);
    return mergedResults;
  }

  @GET
  @Path("summary/function/{functionId}")
  public List<AnomalyMergedResult> getSummaryForFunction(@PathParam("functionId") Long functionId,
      @QueryParam("dimensions") String dimensions) {
    return getAnomalySummaryForFunction(functionId, dimensions, new AnomalyMergeConfig());
  }

  @POST
  @Path("summary/function/{functionId}")
  public List<AnomalyMergedResult> getAnomalySummaryForFunction(
      @PathParam("functionId") Long functionId, @QueryParam("dimensions") String dimensions,
      AnomalyMergeConfig mergeConfig) {
    if (mergeConfig == null) {
      mergeConfig = new AnomalyMergeConfig();
    }
    if (functionId == null) {
      throw new IllegalArgumentException("Function id can't be null");
    }

    List<AnomalyResult> anomalies;
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
