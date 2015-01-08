package com.linkedin.thirdeye.bootstrap.rollup.phase1;

import com.linkedin.thirdeye.api.StarTreeConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * 
 * @author kgopalak
 *
 */
public class RollupPhaseOneConfig {
  private List<String> dimensionNames;
  private List<String> metricNames;
  private List<String> metricTypes;
  private String thresholdFuncClassName;
  private Map<String,String> thresholdFuncParams;
  /**
   * 
   */
  public RollupPhaseOneConfig(){
    
  }
/**
 * 
 * @param dimensionNames
 * @param metricNames
 * @param metricTypes
 * @param thresholdFuncClassName
 * @param thresholdFuncParams
 */
  public RollupPhaseOneConfig(List<String> dimensionNames,
      List<String> metricNames, List<String> metricTypes,
      String thresholdFuncClassName, Map<String, String> thresholdFuncParams) {
    super();
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.metricTypes = metricTypes;
    this.thresholdFuncClassName = thresholdFuncClassName;
    this.thresholdFuncParams = thresholdFuncParams;
  }

  public List<String> getDimensionNames() {
    return dimensionNames;
  }

  public List<String> getMetricNames() {
    return metricNames;
  }

  public List<String> getMetricTypes() {
    return metricTypes;
  }

  public String getThresholdFuncClassName() {
    return thresholdFuncClassName;
  }

  public Map<String, String> getThresholdFuncParams() {
    return thresholdFuncParams;
  }

  public static RollupPhaseOneConfig fromStarTreeConfig(StarTreeConfig config)
  {
    Map<String, String> rollupFunctionConfig = new HashMap<String, String>();

    if (config.getRollup().getFunctionConfig() != null)
    {
      for (Map.Entry<Object, Object> entry : config.getRollup().getFunctionConfig().entrySet())
      {
        rollupFunctionConfig.put((String) entry.getKey(), (String) entry.getValue());
      }
    }

    return new RollupPhaseOneConfig(config.getDimensionNames(),
                                    config.getMetricNames(),
                                    config.getMetricTypes(),
                                    config.getRollup().getFunctionClass(),
                                    rollupFunctionConfig);
  }
}
