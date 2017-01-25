package com.linkedin.thirdeye.dashboard.resources;

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.linkedin.thirdeye.completeness.checker.DataCompletenessConstants.DataCompletenessAlgorithmName;
import com.linkedin.thirdeye.completeness.checker.PercentCompletenessFunctionInput;

@Path(value = "/data-completeness")
@Produces(MediaType.APPLICATION_JSON)
public class DataCompletenessResource {

  @GET
  @Path(value = "/percent-completeness")
  @Produces(MediaType.APPLICATION_JSON)
  public double getPercentCompleteness(String payload) {

    PercentCompletenessFunctionInput input = PercentCompletenessFunctionInput.fromJson(payload);
    DataCompletenessAlgorithmName algorithm = input.getAlgorithm();
    List<Long> baselineCounts = input.getBaselineCounts();
    Long currentCount = input.getCurrentCount();

    double percentCompleteness = 0;
    double baselineTotalCount = 0;
    switch (algorithm) {
      case WO4W_AVERAGE:
      default:
        for (Long baseline : baselineCounts) {
          baselineTotalCount = baselineTotalCount + baseline;
        }
        baselineTotalCount = baselineTotalCount/baselineCounts.size();
        break;
    }
    if (baselineTotalCount != 0) {
      percentCompleteness = new Double(currentCount * 100) / baselineTotalCount;
    }
    return percentCompleteness;
  }


}
