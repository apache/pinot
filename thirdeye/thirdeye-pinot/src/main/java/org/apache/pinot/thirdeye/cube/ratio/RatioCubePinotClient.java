package org.apache.pinot.thirdeye.cube.ratio;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.thirdeye.cube.data.dbrow.DimensionValues;
import org.apache.pinot.thirdeye.cube.data.dbrow.Dimensions;
import org.apache.pinot.thirdeye.cube.data.dbclient.CubeTag;
import org.apache.pinot.thirdeye.cube.data.dbclient.BaseCubePinotClient;
import org.apache.pinot.thirdeye.cube.data.dbclient.CubeSpec;
import org.apache.pinot.thirdeye.datasource.cache.QueryCache;


public class RatioCubePinotClient extends BaseCubePinotClient<RatioRow> {
  private String numeratorMetric = "";
  private String denominatorMetric = "";

  public RatioCubePinotClient(QueryCache queryCache) {
    super(queryCache);
  }

  public void setNumeratorMetric(String numeratorMetric) {
    this.numeratorMetric = numeratorMetric;
  }

  public void setDenominatorMetric(String denominatorMetric) {
    this.denominatorMetric = denominatorMetric;
  }

  protected List<CubeSpec> getCubeSpecs() {
    List<CubeSpec> cubeSpecs = new ArrayList<>();

    cubeSpecs.add(
        new CubeSpec(CubeTag.BaselineNumerator, numeratorMetric, baselineStartInclusive, baselineEndExclusive));
    cubeSpecs.add(
        new CubeSpec(CubeTag.BaselineDenominator, denominatorMetric, baselineStartInclusive, baselineEndExclusive));
    cubeSpecs.add(new CubeSpec(CubeTag.CurrentNumerator, numeratorMetric, currentStartInclusive, currentEndExclusive));
    cubeSpecs.add(
        new CubeSpec(CubeTag.CurrentDenominator, denominatorMetric, currentStartInclusive, currentEndExclusive));

    return cubeSpecs;
  }

  protected void fillValueToRowTable(Map<List<String>, RatioRow> rowTable, Dimensions dimensions,
      List<String> dimensionValues, double value, CubeTag tag) {

    if (Double.compare(0d, value) < 0 && !Double.isInfinite(value)) {
      RatioRow row = rowTable.get(dimensionValues);
      if (row == null) {
        row = new RatioRow(dimensions, new DimensionValues(dimensionValues));
        rowTable.put(dimensionValues, row);
      }
      switch (tag) {
        case BaselineNumerator:
          row.setBaselineValue(value);
          break;
        case BaselineDenominator:
          row.setBaselineDenominatorValue(value);
          break;
        case CurrentNumerator:
          row.setCurrentValue(value);
          break;
        case CurrentDenominator:
          row.setCurrentDenominatorValue(value);
          break;
        default:
      }
    }
  }
}
