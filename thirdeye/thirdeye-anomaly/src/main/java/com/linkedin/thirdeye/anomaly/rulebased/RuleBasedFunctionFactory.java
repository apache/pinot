package com.linkedin.thirdeye.anomaly.rulebased;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.api.AnomalyDatabaseConfig;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionFunctionFactory;
import com.linkedin.thirdeye.anomaly.api.FunctionProperties;
import com.linkedin.thirdeye.anomaly.api.external.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomaly.database.DeltaTable;
import com.linkedin.thirdeye.anomaly.database.FunctionTableRow;
import com.linkedin.thirdeye.anomaly.exception.IllegalFunctionException;
import com.linkedin.thirdeye.anomaly.util.DimensionKeyMatchTable;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeGranularity;

/**
 * Loads rules from the function table in the anomaly database.
 */
public class RuleBasedFunctionFactory extends AnomalyDetectionFunctionFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(RuleBasedFunctionFactory.class);

  /**
   * Supported rule types
   */
  private enum RuleTypes {
    PERCENTCHANGE, ABSOLUTECHANGE,
  }

  @Override
  public Class<? extends FunctionTableRow> getFunctionRowClass() {
    return RuleBasedFunctionTableRow.class;
  }

  @Override
  public AnomalyDetectionFunction getFunction(StarTreeConfig starTreeConfig, AnomalyDatabaseConfig dbconfig,
      FunctionTableRow functionTableRow) throws IllegalFunctionException, ClassCastException {
    RuleBasedFunctionTableRow ruleBasedFunctionTableRow = (RuleBasedFunctionTableRow) functionTableRow;

    /*
     * Begin: perform some basic rule validation
     */
    TimeGranularity aggregateGranularity = new TimeGranularity(ruleBasedFunctionTableRow.getAggregateSize(),
        TimeUnit.valueOf(ruleBasedFunctionTableRow.getAggregateUnit()));
    if (aggregateGranularity.getSize() <= 0) {
      throw new IllegalFunctionException("aggregate size must be positive");
    }
    TimeGranularity baselineGranularity = new TimeGranularity(ruleBasedFunctionTableRow.getBaselineSize(),
        TimeUnit.valueOf(ruleBasedFunctionTableRow.getBaselineUnit()));
    if (baselineGranularity.getSize() <= 0) {
      throw new IllegalFunctionException("baseline size must be positive");
    }
    /*
     * End: basic rule validation
     */

    DimensionKeyMatchTable<Double> deltaTable = null;
    if (ruleBasedFunctionTableRow.getDeltaTableName() != null) {
      deltaTable = DeltaTable.load(dbconfig, starTreeConfig, ruleBasedFunctionTableRow.getDeltaTableName());
    }

    AnomalyDetectionFunction func;
    switch (RuleTypes.valueOf(ruleBasedFunctionTableRow.getFunctionName().toUpperCase())) {
      case PERCENTCHANGE:
      {
        func = new AnomalyDetectionFunctionPercentChange(baselineGranularity, aggregateGranularity,
            ruleBasedFunctionTableRow.getMetricName(), ruleBasedFunctionTableRow.getDelta())
          .setDeltaTable(deltaTable);
        break;
      }
      case ABSOLUTECHANGE:
      {
        func = new AnomalyDetectionFunctionAbsoluteChange(baselineGranularity, aggregateGranularity,
            ruleBasedFunctionTableRow.getMetricName(), ruleBasedFunctionTableRow.getDelta())
          .setDeltaTable(deltaTable);
        break;
      }
      default:
      {
        throw new IllegalFunctionException("no rule of type " + ruleBasedFunctionTableRow.getFunctionName());
      }
    }

    // wrap function in consecutive function
    int consecutiveBuckets = ruleBasedFunctionTableRow.getConsecutiveBuckets();
    if (consecutiveBuckets > 1) {
      func = new AnomalyDetectionFunctionConsecutive(func, consecutiveBuckets);
    }

    // wrap function in cron function
    String cronDefinition = ruleBasedFunctionTableRow.getCronDefinition();
    if (cronDefinition != null && cronDefinition.length() > 0) {
      func = new AnomalyDetectionFunctionCronDefinition(func, cronDefinition);
    }

    /*
     * RuleBased anomaly detection does not use init()
     */
    func.init(starTreeConfig, new FunctionProperties());

    LOGGER.info("Loaded rule: {}", func); // recursively log a semi-human readable rule description
    return func;
  }

}
