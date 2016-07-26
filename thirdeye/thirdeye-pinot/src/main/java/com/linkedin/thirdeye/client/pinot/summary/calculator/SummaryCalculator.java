package com.linkedin.thirdeye.client.pinot.summary.calculator;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;

import com.linkedin.thirdeye.client.pinot.summary.Cube;
import com.linkedin.thirdeye.client.pinot.summary.Record;
import com.linkedin.thirdeye.client.pinot.summary.Summary;


public class SummaryCalculator {
  private final static String iFileName = "Cube.json";

  private final static int answerSize = 4;
  private final static boolean doSorting = true;
  private final static int sortBy = 1; // 0: ratio, 1: diff, 2: cost
  private final static boolean isDescendant = true;
  private final static boolean removeEmptyRecords = true; // remove the record that contains ta or tb = 0

  public static Summary computeSummary(Cube records, int N) {
    preprocessingCube(records);
    DPArray dp = new DPArray(N, records.getRg(), records.getGa(), records.getGb(), records.size());

    for (int idx = 0; idx < records.size(); ++idx) {
      // println: the answer of each iteration
      //      System.out.println(idx + ":");
      //      System.out.println(dp);

      Record tab = records.get(idx);
      double tabCost = err(tab.metricA, tab.metricB, dp.get(0).ratio);

      for (int n = N; n > 0; --n) {
        double val1 = dp.get(n - 1).cost;
//        double val2 = dp.get(n).cost + tabCost; // fixed r per iteration
        double val2 = dp.get(n).cost + err(tab.metricA, tab.metricB, dp.get(n).ratio); // dynamic r
        if (Double.compare(val1, val2) < 0) {
          dp.get(n).cost = val1;
          dp.get(n).ans.and(dp.get(n - 1).ans); // dp[n].ans = dp[n-1].ans
          dp.get(n).ans.or(dp.get(n - 1).ans);
          dp.get(n).ans.set(idx);
          dp.get(n).subMetricA = dp.get(n - 1).subMetricA - tab.metricA;
          dp.get(n).subMetricB = dp.get(n - 1).subMetricB - tab.metricB;
          dp.get(n).ratio = dp.get(n).subMetricB / dp.get(n).subMetricA;
        } else {
          // subMetricA, subMetricB, and ratio remain the same
          dp.get(n).cost = val2;
        }
      }
      // update ratio for the next iteration
      dp.get(0).ratio = dp.get(N).subMetricB / dp.get(N).subMetricA;
      dp.get(0).cost = dp.get(0).cost + err(tab.metricA, tab.metricB, dp.get(0).ratio);
    }

    // println: the final answer
    //    System.out.println("Answer:");
    //    System.out.println(dp);

    return new Summary(records, dp);
  }

  public static double err(double va, double vb, double r) {
    // TODO incremental calculation of err
    return (vb - r * va) * Math.log(vb / (r * va));
  }

  private static void preprocessingCube(Cube records) {
    if (removeEmptyRecords)
      records.removeEmptyRecords();

    Comparator<Record> comparator = null;
    if (doSorting) {
      switch (sortBy) {
        case 0:
          comparator = new RatioComparator();
          break;
        case 1:
          comparator = new DiffComparator();
          break;
        case 2:
        default:
          comparator = new CostComparator(records.getRg());
      }
    }
    if (comparator != null) {
      if (isDescendant) {
        comparator = Collections.reverseOrder(comparator);
      }
      records.sort(comparator);
    }
  }

  static class RatioComparator implements Comparator<Record> {
    @Override
    public int compare(Record t1, Record t2) {
      return Double.compare(t1.metricB / t1.metricA, t2.metricB / t2.metricA); // sort by ratio
    }
  }

  static class DiffComparator implements Comparator<Record> {
    @Override
    public int compare(Record t1, Record t2) {
      return Double.compare(Math.abs(t1.metricB - t1.metricA), Math.abs(t2.metricB - t2.metricA)); // sort by diff
    }
  }

  static class CostComparator implements Comparator<Record> {
    double ratio = 1.;

    public CostComparator(double ratio) {
      this.ratio = ratio;
    }

    @Override
    public int compare(Record t1, Record t2) {
      return Double.compare(err(t1.metricA, t1.metricB, ratio), err(t2.metricA, t2.metricB, ratio)); // sort by ratio
    }

    private double err(double va, double vb, double r) {
      return (vb - r * va) * Math.log(vb / (r * va));
    }
  }

  public static void main(String[] argc) {
    try {
      Cube records = Cube.fromJson(iFileName);
      System.out.println("Records:");
      System.out.println(records);

      Summary summary = computeSummary(records, answerSize);
      System.out.println(summary.toString());
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
