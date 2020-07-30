package org.apache.pinot.thirdeye.dashboard.resources.v2.anomalies;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import java.util.List;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.thirdeye.api.Constants;


/**
 * The type Anomaly search resource.
 */
@Path(value = "/anomaly-search")
@Produces(MediaType.APPLICATION_JSON)
@Api(tags = {Constants.DETECTION_TAG})
public class AnomalySearchResource {

  private final AnomalySearcher anomalySearcher;

  /**
   * Instantiates a new Anomaly search resource.
   */
  public AnomalySearchResource() {
    this.anomalySearcher = new AnomalySearcher();
  }

  /**
   * Search and paginate the anomalies according to the parameters.
   *
   * @param limit the limit
   * @param offset the offset
   * @param startTime the start time
   * @param endTime the end time
   * @param feedbacks the feedback types, e.g. ANOMALY, NOT_ANOMALY
   * @param subscriptionGroups the subscription groups
   * @param detectionNames the detection names
   * @param metrics the metrics
   * @param datasets the datasets
   * @param anomalyIds the anomaly ids
   * @return the response
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation("Search and paginate anomalies according to the parameters")
  public Response findAlerts(@QueryParam("limit") @DefaultValue("10") int limit,
      @QueryParam("offset") @DefaultValue("0") int offset, @QueryParam("startTime") Long startTime,
      @QueryParam("endTime") Long endTime, @QueryParam("feedbackStatus") List<String> feedbacks,
      @QueryParam("subscriptionGroup") List<String> subscriptionGroups,
      @QueryParam("detectionName") List<String> detectionNames, @QueryParam("metric") List<String> metrics,
      @QueryParam("dataset") List<String> datasets, @QueryParam("anomalyId") List<Long> anomalyIds) {
    AnomalySearchFilter searchFilter =
        new AnomalySearchFilter(startTime, endTime, feedbacks, subscriptionGroups, detectionNames, metrics, datasets, anomalyIds);
    return Response.ok().entity(this.anomalySearcher.search(searchFilter, limit, offset)).build();
  }
}
