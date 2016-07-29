package com.linkedin.thirdeye.client.pinot.slsummary;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;


public class Summary {
  double Ga;
  double Gb;
  double Rg;
  double totalCost;

  List<Cell> resRecords = new ArrayList<>();

  public Summary(Cube cube, DPArray dp) {
    this.Ga = dp.get(dp.size() - 1).getSubMetricA();
    this.Gb = dp.get(dp.size() - 1).getSubMetricB();
    this.Rg = dp.get(0).getRatio();
//    this.totalCost = dp.get(dp.size() - 1).getCost();
    this.totalCost = .0;

    BitSet ans = dp.getAnswer();
//    for (int i = ans.nextSetBit(0); i >= 0; i = ans.nextSetBit(i + 1)) {
    for (int i = 0; i < cube.size(); ++i) {
      if (ans.get(i)) this.resRecords.add(new Cell(cube.get(i)));
      else {
        this.totalCost += SummaryCalculator.err(cube.get(i).metricA, cube.get(i).metricB, this.Rg);
      }
    }
  }

  public String toString() {
    String dimensionSpace = "%-16s";
    String metricSpace = "%-8s";
    DecimalFormat dFormat = new DecimalFormat("0.000");

    StringBuilder res = new StringBuilder("Summary:\n");
    // header
    res.append(String.format(dimensionSpace, "Name"));
    res.append('\t');
    res.append(String.format(metricSpace, "ta"));
    res.append('\t');
    res.append(String.format(metricSpace, "tb"));
    res.append('\t');
    res.append(String.format(metricSpace, "ratio"));
    res.append('\t');
    res.append(String.format(metricSpace, "cost"));
    res.append('\n');

    // (ALL)- row
    res.append(String.format(dimensionSpace, "(ALL)-"));
    res.append('\t');
    res.append(String.format(metricSpace, (int) Ga));
    res.append('\t');
    res.append(String.format(metricSpace, (int) Gb));
    res.append('\t');
    res.append(String.format(metricSpace, dFormat.format(Rg)));
    res.append('\t');
    res.append(String.format(metricSpace, dFormat.format(totalCost)));
    res.append('\n');

    boolean dir = Double.compare(1.0, Rg) <= 0;
    StringBuilder nres = new StringBuilder();
    StringBuilder sb = res;
    for (Cell cell : resRecords) {
      sb = dir & (Double.compare(1.0, cell.ratio) <= 0) ? res : nres;
      sb.append(String.format(dimensionSpace, cell.dimensionName + '/' + cell.dimensionValue));
      sb.append('\t');
      sb.append(String.format(metricSpace, (int) cell.metricA));
      sb.append('\t');
      sb.append(String.format(metricSpace, (int) cell.metricB));
      sb.append('\t');
      sb.append(String.format(metricSpace, dFormat.format(cell.ratio)));
      sb.append('\n');
    }

    res.append(nres.toString());

    return res.toString();
  }

  static class Cell {
    String dimensionName;
    String dimensionValue;
    double metricA;
    double metricB;
    double ratio;

    Cell(Record record) {
      this.dimensionName = record.dimensionName;
      this.dimensionValue = record.dimensionValue;
      this.metricA = record.metricA;
      this.metricB = record.metricB;
      this.ratio = record.metricB / record.metricA;
    }
  }
}
