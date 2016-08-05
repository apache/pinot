package com.linkedin.thirdeye.dashboard.resources;

import com.linkedin.thirdeye.anomaly.merge.AnomalyMergeConfig;
import com.linkedin.thirdeye.anomaly.merge.AnomalyMergeStrategy;
import com.linkedin.thirdeye.anomaly.merge.AnomalySummaryGenerator;
import com.linkedin.thirdeye.api.dto.GroupByKey;
import com.linkedin.thirdeye.api.dto.GroupByRow;
import com.linkedin.thirdeye.api.dto.MergedAnomalyResult;
import com.linkedin.thirdeye.db.dao.AnomalyResultDAO;
import com.linkedin.thirdeye.db.entity.AnomalyResult;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;

@Path("thirdeye/anomaly")
@Produces(MediaType.APPLICATION_JSON)
public class AnomalySummaryResource {

  AnomalyResultDAO resultDAO;

  public AnomalySummaryResource(AnomalyResultDAO resultDAO) {
    this.resultDAO = resultDAO;
  }

  @GET
  @Path("merge/function")
  public List<MergedAnomalyResult> getMergedAnomaliesByFunction(
      @QueryParam("functionId") Long functionId, @QueryParam("startTime") Long startTime,
      @QueryParam("endTime") Long endTime) {
    if (functionId == null) {
      throw new IllegalArgumentException("functionId can't be null");
    }
    if (startTime == null) {
      // show from beginning
      startTime = 0l;
    }
    if (endTime == null) {
      endTime = System.currentTimeMillis();
    }
    List<AnomalyResult> anomalies = resultDAO
        .findAllByTimeAndFunctionId(new DateTime(startTime), new DateTime(endTime), functionId);
    AnomalyMergeConfig defaultMergeConfig = new AnomalyMergeConfig();
    return AnomalySummaryGenerator.mergeAnomalies(anomalies, defaultMergeConfig);
  }

  @GET
  @Path("merge/collection")
  public List<MergedAnomalyResult> getMergedAnomaliesByMetric(
      @QueryParam("collection") String collection, @QueryParam("metric") String metric,
      @QueryParam("startTime") Long startTime, @QueryParam("endTime") Long endTime) {
    if (StringUtils.isBlank(collection)) {
      throw new IllegalArgumentException("Collection can't be empty");
    }
    if (startTime == null) {
      // show from beginning
      startTime = 0l;
    }
    if (endTime == null) {
      endTime = System.currentTimeMillis();
    }
    DateTime startTimeUtc = new DateTime(startTime);
    DateTime endTimeUtc = new DateTime(endTime);

    List<AnomalyResult> anomalies;
    if (!StringUtils.isEmpty(metric)) {
      anomalies =
          resultDAO.findAllByCollectionTimeAndMetric(collection, metric, startTimeUtc, endTimeUtc);
    } else {
      anomalies = resultDAO.findAllByCollectionAndTime(collection, startTimeUtc, endTimeUtc);
    }
    AnomalyMergeConfig defaultMergeConfig = new AnomalyMergeConfig();
    return AnomalySummaryGenerator.mergeAnomalies(anomalies, defaultMergeConfig);
  }

  @GET
  @Path("groupBy/{mergeStrategy}")
  public List<GroupByRow<GroupByKey, Long>> getAnomalyDataGroupedByFunction(
      @PathParam("mergeStrategy") AnomalyMergeStrategy mergeStrategy) {
    switch (mergeStrategy) {
    case FUNCTION:
      return resultDAO.getCountByFunction();
    case COLLECTION_METRIC:
      return resultDAO.getCountByCollectionMetric();
    case COLLECTION:
      return resultDAO.getCountByCollection();
    default:
      throw new IllegalArgumentException("Unknown merge strategy : " + mergeStrategy);
    }
  }
}
