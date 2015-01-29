package com.linkedin.thirdeye;

import com.linkedin.thirdeye.api.DimensionKey;

import java.util.Map;
import java.util.Properties;

public class AnomalyResult
{
  private Map<Long, Double> pValues;
  private DimensionKey dimensionKey;
  private Properties functionParameters;

  public AnomalyResult() {}

  public Map<Long, Double> getPValues()
  {
    return pValues;
  }

  public void setPValues(Map<Long, Double> pValues)
  {
    this.pValues = pValues;
  }

  public DimensionKey getDimensionKey()
  {
    return dimensionKey;
  }

  public void setDimensionKey(DimensionKey dimensionKey)
  {
    this.dimensionKey = dimensionKey;
  }

  public Properties getFunctionParameters()
  {
    return functionParameters;
  }

  public void setFunctionParameters(Properties functionParameters)
  {
    this.functionParameters = functionParameters;
  }
}
