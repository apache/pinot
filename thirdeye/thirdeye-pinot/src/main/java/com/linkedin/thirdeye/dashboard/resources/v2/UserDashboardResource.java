package com.linkedin.thirdeye.dashboard.resources.v2;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.constant.AnomalyResultSource;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.AnomalySummary;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;


/**
 * Endpoints for user-customized dashboards (currently alerts only)
 */
@Path(value = "/userdashboard")
@Produces(MediaType.APPLICATION_JSON)
public class UserDashboardResource {
  private final MergedAnomalyResultManager anomalyDAO;
  private final AnomalyFunctionManager functionDAO;
  private final MetricConfigManager metricDAO;

  public UserDashboardResource(MergedAnomalyResultManager anomalyDAO, AnomalyFunctionManager functionDAO, MetricConfigManager metricDAO) {
    this.anomalyDAO = anomalyDAO;
    this.functionDAO = functionDAO;
    this.metricDAO = metricDAO;
  }

  /**
   * Returns a list of AnomalySummary for a set of query parameters. Anomalies are
   * sorted by start time (descending).
   *
   * <br/><b>Example:</b>
   * <pre>
   *   [ {
   *     "id" : 12345,
   *     "start" : 1517000000000,
   *     "end" : 1517100000000,
   *     "dimensions" : { "country": "us" },
   *     "severity" : 0.517,
   *     "current" : 1213.0,
   *     "baseline" : 550.0,
   *     "feedback" : "NO_FEEDBACK",
   *     "metricId" : 12346
   *     "metric" : "page_views",
   *     "functionId" : 12347
   *     "functionName" : "page_views_monitoring"
   *     },
   *     ...
   *   ]
   * </pre>
   *
   * @see AnomalySummary
   *
   * @param start window start time
   * @param end window end time (optional)
   * @param owner anomaly function owner only (optional)
   * @param application anomaly function for application alert groups only (optional)
   *
   * @return List of AnomalySummary
   */
  @GET
  @Path("/anomalies")
  public List<AnomalySummary> queryAnomalies(
      @QueryParam("start") Long start,
      @QueryParam("end") Long end,
      @QueryParam("owner") String owner,
      @QueryParam("application") String application,
      @QueryParam("limit") Integer limit) {

    //
    // query params
    //

    // safety condition
    Preconditions.checkNotNull(start);

    List<Predicate> predicates = new ArrayList<>();

    // TODO support index select on user-reported anomalies
//    predicates.add(Predicate.OR(
//        Predicate.EQ("notified", true),
//        Predicate.EQ("anomalyResultSource", AnomalyResultSource.USER_LABELED_ANOMALY)));


    // application (indirect)
    Set<Long> applicationFunctionIds = new HashSet<>();
    if (application != null) {
      List<AnomalyFunctionDTO> functions = this.functionDAO.findAllByApplication(application);
      for (AnomalyFunctionDTO function : functions) {
        applicationFunctionIds.add(function.getId());
      }
    }

    // owner (indirect)
    Set<Long> ownerFunctionIds = new HashSet<>();
    if (owner != null) {
      // TODO: replace database scan with targeted select
      List<AnomalyFunctionDTO> functions = this.functionDAO.findAll();
      for (AnomalyFunctionDTO function : functions) {
        if (Objects.equals(function.getCreatedBy(), owner)) {
          ownerFunctionIds.add(function.getId());
        }
      }
    }

    // anomaly function ids
    if (application != null || owner != null) {
      Set<Long> functionIds = new HashSet<>();
      functionIds.addAll(applicationFunctionIds);
      functionIds.addAll(ownerFunctionIds);

      predicates.add(Predicate.IN("functionId", functionIds.toArray()));
    }

    // anomaly window start
    if (start != null) {
      predicates.add(Predicate.GT("endTime", start));
    }

    // anomaly window end
    if (end != null) {
      predicates.add(Predicate.LT("startTime", end));
    }

    //
    // fetch anomalies
    //
    List<MergedAnomalyResultDTO> anomalies = this.anomalyDAO.findByPredicate(Predicate.AND(predicates.toArray(new Predicate[predicates.size()])));

    // filter (un-notified && non-user-reported) anomalies
    // TODO remove once index select on user-reported anomalies available
    Iterator<MergedAnomalyResultDTO> itAnomaly = anomalies.iterator();
    while (itAnomaly.hasNext()) {
      MergedAnomalyResultDTO anomaly = itAnomaly.next();
      if (!anomaly.isNotified() &&
          !AnomalyResultSource.USER_LABELED_ANOMALY.equals(anomaly.getAnomalyResultSource())) {
        itAnomaly.remove();
      }
    }

    // sort descending by start time
    Collections.sort(anomalies, new Comparator<MergedAnomalyResultDTO>() {
      @Override
      public int compare(MergedAnomalyResultDTO o1, MergedAnomalyResultDTO o2) {
        return -1 * Long.compare(o1.getStartTime(), o2.getStartTime());
      }
    });

    // limit result size
    if (limit != null) {
      anomalies = anomalies.subList(0, limit);
    }

    //
    // fetch functions
    //
    Set<Long> anomalyFunctionIds = new HashSet<>();
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      anomalyFunctionIds.add(anomaly.getFunctionId());
    }

    List<AnomalyFunctionDTO> functions = this.functionDAO.findByPredicate(Predicate.IN("baseId", anomalyFunctionIds.toArray()));
    Map<Long, AnomalyFunctionDTO> id2function = new HashMap<>();
    for (AnomalyFunctionDTO function : functions) {
      id2function.put(function.getId(), function);
    }

    //
    // format output
    //
    List<AnomalySummary> output = new ArrayList<>();
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      AnomalySummary summary = new AnomalySummary();
      summary.setId(anomaly.getId());
      summary.setStart(anomaly.getEndTime());
      summary.setEnd(anomaly.getEndTime());
      summary.setCurrent(anomaly.getAvgCurrentVal());
      summary.setBaseline(anomaly.getAvgBaselineVal());
      summary.setMetricId(getMetricId(anomaly));
      summary.setMetricName(anomaly.getMetric());
      summary.setDimensions(anomaly.getDimensions());
      summary.setDataset(anomaly.getCollection());

      // TODO use alert filter if necessary
      summary.setSeverity(Math.abs(anomaly.getWeight()));

      summary.setFunctionId(anomaly.getFunctionId());
      if (id2function.get(anomaly.getFunctionId()) != null) {
        summary.setFunctionName(id2function.get(anomaly.getFunctionId()).getFunctionName());
      }

      summary.setFeedback(AnomalyFeedbackType.NO_FEEDBACK);
      if (anomaly.getFeedback() != null) {
        summary.setFeedback(anomaly.getFeedback().getFeedbackType());
      }

      output.add(summary);
    }

    return output;
  }

  /**
   * Helper to work around for anomaly function not setting metric id
   *
   * @param anomaly anomaly dto
   * @return metric id, or {@code -1} if the metric/dataset cannot be resolved
   */
  private long getMetricId(MergedAnomalyResultDTO anomaly) {
    if (anomaly.getFunction().getMetricId() > 0) {
      return anomaly.getFunction().getMetricId();
    }
    try {
      return this.metricDAO.findByMetricAndDataset(anomaly.getMetric(), anomaly.getCollection()).getId();
    } catch (Exception e) {
      return -1;
    }
  }
}
