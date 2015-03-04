package com.linkedin.thirdeye.bootstrap.rollup;

import java.util.Map;
import java.util.Set;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.RollupThresholdFunction;
/*
 * @author ajaspal
 *
 * This Function is used to check for thresholds w.r.t. multiple metrics.
 * Configuration e.g.
 *
 *  rollup:
        functionClass: com.linkedin.thirdeye.bootstrap.rollup.MultiMetricTotalAggregateBasedRollupFunction
        functionConfig:
          metricNames: "numberOfMemberConnectionsSent,numberOfGuestInvitationsSent"
          thresholdExpr: "(numberOfMemberConnectionsSent > 5000 || numberOfGuestInvitationsSent > 1000)"

 * metricNames -> represent a comma separated string of metrics in the threshold expression.
 * thresholdExpr -> a string containing valid boolean expression
 * TODO: implement some parsing logic to extract the metric names from the threshold expression
 *       rather than forcing the user to supply them explicitly.
 */
import com.linkedin.thirdeye.api.TimeGranularity;

public class MultiMetricTotalAggregateBasedRollupFunction implements RollupThresholdFunction
{

  private static final Logger LOG = LoggerFactory.getLogger(MultiMetricTotalAggregateBasedRollupFunction.class);
  private String[] metricNames;
  private String thresholdExpr;
  public MultiMetricTotalAggregateBasedRollupFunction(Map<String, String> params){
    this.metricNames = params.get("metricNames").split(",");
    this.thresholdExpr = params.get("thresholdExpr");
  }
  /**
   *
   */
  @Override
  public boolean isAboveThreshold(MetricTimeSeries timeSeries)
  {
    Set<Long> timeWindowSet = timeSeries.getTimeWindowSet();
    JexlEngine jexl = new JexlEngine();
    JexlContext context = new MapContext();
    Expression e = jexl.createExpression(this.thresholdExpr);
    for(String metricName : metricNames){
      long sum = 0;
      for (Long timeWindow : timeWindowSet) {
        sum += timeSeries.get(timeWindow, metricName).longValue();
      }
      context.set(metricName, sum);
      if (LOG.isDebugEnabled()) {
        LOG.debug(metricName + " = " + sum);
      }
    }
    return ((Boolean)e.evaluate(context)).booleanValue();
  }

  @Override
  public TimeGranularity getRollupAggregationGranularity(){
    return null;
  }
}

