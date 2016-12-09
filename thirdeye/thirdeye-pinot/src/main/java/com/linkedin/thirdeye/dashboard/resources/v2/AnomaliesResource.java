package com.linkedin.thirdeye.dashboard.resources.v2;

import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import jersey.repackaged.com.google.common.collect.Lists;

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;

@Path(value = "/anomalies")
@Produces(MediaType.APPLICATION_JSON)
public class AnomaliesResource {

  public static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private final MetricConfigManager metricConfigDAO;
  private final MergedAnomalyResultManager mergedAnomalyResultDAO;

  public AnomaliesResource() {
    metricConfigDAO = DAO_REGISTRY.getMetricConfigDAO();
    mergedAnomalyResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
  }

  @GET
  @Path("metric/{metricId}/{startTime}/{endTime}")
  public List<MergedAnomalyResultDTO> getAnomaliesForMetricInRange(
      @PathParam("metricId") Long metricId,
      @PathParam("startTime") Long startTime,
      @PathParam("endTime") Long endTime) {

    MetricConfigDTO metricConfig = metricConfigDAO.findById(metricId);
    String collection = metricConfig.getDataset();
    String metric = metricConfig.getName();
    List<MergedAnomalyResultDTO> mergedAnomalies =
        mergedAnomalyResultDAO.findByCollectionMetricTime(collection, metric, startTime, endTime, false);
    return mergedAnomalies;
  }


  @GET
  @Path("count/{metricId}/{startTime}/{endTime}/{numBuckets}")
  public List<Integer> getAnomalyCountForMetricInRange(
      @PathParam("metricId") Long metricId,
      @PathParam("startTime") Long startTime,
      @PathParam("endTime") Long endTime,
      @PathParam("numBuckets") int numBuckets) {

    Integer[] anomalyCount = new Integer[numBuckets];
    List<MergedAnomalyResultDTO> mergedAnomalies = getAnomaliesForMetricInRange(metricId, startTime, endTime);

    long bucketSize = (endTime - startTime) / numBuckets;
    for (MergedAnomalyResultDTO mergedAnomaly : mergedAnomalies) {
      Long anomalyStartTime = mergedAnomaly.getStartTime();
      Integer bucketNumber = (int) ((anomalyStartTime - startTime) / bucketSize);
      anomalyCount[bucketNumber] ++;
    }
    return Lists.newArrayList(anomalyCount);
  }



}
