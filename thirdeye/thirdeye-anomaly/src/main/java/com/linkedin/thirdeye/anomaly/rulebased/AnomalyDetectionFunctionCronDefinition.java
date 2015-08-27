package com.linkedin.thirdeye.anomaly.rulebased;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.api.FunctionProperties;
import com.linkedin.thirdeye.anomaly.api.function.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomaly.api.function.AnomalyResult;
import com.linkedin.thirdeye.anomaly.api.function.exception.IllegalFunctionException;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeRange;

/**
 * Uses quartz job scheduler cron expression format.
 */
public class AnomalyDetectionFunctionCronDefinition extends AnomalyDetectionFunctionAbstractWrapper {

  private static final Logger LOGGER = LoggerFactory.getLogger(AnomalyDetectionFunctionCronDefinition.class);

  private final TimeZone evalTimeZone;;

  private final CronExpression cronExpression;

  public AnomalyDetectionFunctionCronDefinition(AnomalyDetectionFunction childFunc, String cronString)
      throws IllegalFunctionException {
    super(childFunc);

    /*
     *  Use local timezone of the system.
     */
    evalTimeZone = TimeZone.getDefault();

    try {
      cronExpression = new CronExpression(cronString);
      cronExpression.setTimeZone(evalTimeZone);
    } catch (ParseException e) {
      throw new IllegalFunctionException("Invalid cron definition for rule");
    }
  }

  @Override
  public void init(StarTreeConfig starTreeConfig, FunctionProperties functionConfig) throws IllegalFunctionException {
    super.init(starTreeConfig, functionConfig);
  }

  @Override
  public TimeGranularity getTrainingWindowTimeGranularity() {
    return childFunc.getTrainingWindowTimeGranularity();
  }

  @Override
  public List<AnomalyResult> analyze(DimensionKey dimensionKey, MetricTimeSeries series, TimeRange timeInterval,
      List<AnomalyResult> anomalyHistory) {
    List<AnomalyResult> timeFilteredResults = new ArrayList<AnomalyResult>();
    List<AnomalyResult> intermediateResults = childFunc.analyze(dimensionKey, series, timeInterval, anomalyHistory);

    // remove results that do not match the cron expression
    for (AnomalyResult intermediateResult : intermediateResults) {
      DateTime resultDtUTC = new DateTime(intermediateResult.getTimeWindow(), DateTimeZone.UTC);
      DateTime resultDtEvalTz= resultDtUTC.withZone(DateTimeZone.forTimeZone(evalTimeZone));
      Date dateEvalTz = resultDtEvalTz.toDate();
      if (cronExpression.isSatisfiedBy(dateEvalTz)) {
        timeFilteredResults.add(intermediateResult);
      }
    }
    return timeFilteredResults;
  }

  public String toString() {
    return String.format("%s for cron def '%s' ", childFunc.toString(), cronExpression.getCronExpression());
  }

}
