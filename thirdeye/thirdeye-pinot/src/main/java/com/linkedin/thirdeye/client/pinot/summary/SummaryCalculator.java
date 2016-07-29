package com.linkedin.thirdeye.client.pinot.summary;

import com.linkedin.thirdeye.client.pinot.slsummary.DPArray;

public class SummaryCalculator {
  public static Summary computeSummary(Cube cube, int N) {

    DPArray dp =
        new DPArray(N, cube.getTopRatio(), cube.getTopBaselineValue(), cube.getTopCurrentValue(), cube.getRowCount());

    for (int idx = 0; idx < cube.getRowCount(); ++idx) {
      // println: the answer of each iteration
      //      System.out.println(idx + ":");
      //      System.out.println(dp);

      Row row = cube.getRowAt(idx);
      double rowCost = CostFunction.err(row.getBaselineValue(), row.getCurrentValue(), dp.get(0).getRatio());

      for (int n = N; n > 0; --n) {
        double val1 = dp.get(n - 1).getCost();
        double val2 = dp.get(n).getCost() + rowCost; // fixed r per iteration
//        double val2 = dp.get(n).getCost() + CostFunction.err(tab.getBaselineValue(), tab.getCurrentValue(), dp.get(n).getRatio()); // dynamic r
        if (Double.compare(val1, val2) < 0) {
          dp.get(n).setCost(val1);
          dp.get(n).getAns().and(dp.get(n - 1).getAns()); // dp[n].ans = dp[n-1].ans
          dp.get(n).getAns().or(dp.get(n - 1).getAns());
          dp.get(n).getAns().set(idx);
          dp.get(n).setSubMetricA(dp.get(n - 1).getSubMetricA() - row.getBaselineValue());
          dp.get(n).setSubMetricB(dp.get(n - 1).getSubMetricB() - row.getCurrentValue());
          dp.get(n).setRatio(dp.get(n).getSubMetricB() / dp.get(n).getSubMetricA());
        } else {
          // subMetricA, subMetricB, and ratio remain the same
          dp.get(n).setCost(val2);
        }
      }
      // update ratio for the next iteration
      dp.get(0).setRatio(dp.get(N).getSubMetricB() / dp.get(N).getSubMetricA());
      dp.get(0).setCost(dp.get(0).getCost() + CostFunction.err(row.getBaselineValue(), row.getCurrentValue(), dp.get(0).getRatio()));
    }

    // println: the final answer
        System.out.println("Answer:");
        System.out.println(dp);

    return new Summary(cube, dp);
  }
}
