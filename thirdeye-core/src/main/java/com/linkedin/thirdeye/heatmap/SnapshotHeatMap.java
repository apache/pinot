package com.linkedin.thirdeye.heatmap;

import java.util.*;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/**
 * SnapshotHeatMap class is designed to discover the informative components in aggregate time series.
 * The class takes the "snapshot" at two different time stamps, the output tries to explain the difference
 * or commonality of the aggregate automatically by measuring the compression loss.
 * The steps to use the class, firstly, initialize a class object of Snapshot. Secondly, call the updateRecords()
 * function to get the output.
 * @author jjchen
 * Date: 01/26/2015
 */

public class SnapshotHeatMap implements HeatMap
{
    private static final String COLOR = "#888AFC";
    private static final String DOWNCOLOR = "#8afc88";
    private static final String UPCOLOR = "#fc888a";

    protected Map<String, double[]> _snapshotDictionary;
    //_MaxRecords is an input variable. It indicates how many Records will be output.
    protected int _maxRecords;
    //_NRecords indicates how many underlying components are.
    protected int _nRecords;
    //_knob is a parameter to come up a combined metric (ratio, absolute changes).
    protected double _knob;
    // _DimensionValue is a list of underlying components.
    protected String[] _dimensionValue;
    // TableReturn is a class which carries the right lower cell information in the table created by DP.
    protected class TableReturn {
        public String _records;
        public double _tableCost;
    }

    /**
     * Constructor.
     * @param input1: A map of the name of the components and its value at time stamp 1.
     * @param input2: A map of the name of the components and its value at time stamp 2.
     * @param outputRecords: The number of output records.
     * @knobValue: a value to determine the compression loss.
     */
    public SnapshotHeatMap(Map<String, Double> input1, Map<String, Double> input2, int outputRecords, double knobValue)
            throws Exception {

        _snapshotDictionary = new HashMap<String, double[]>();
        _maxRecords = outputRecords;
        _knob = knobValue;
        if (input1.size() != input2.size()) {
            throw new IllegalArgumentException(String.format(
                    "two snapshots should have equivalent length, t1 with size %s and t2 with size %s.",
                    input1.size(),
                    input2.size()));
        }
        for (String keyValue : input1.keySet()) {
            if (!input2.containsKey(keyValue)) {
                throw new IllegalArgumentException(String.format(
                        "two snapshots contains different dimension values. t2 does not contain %s", keyValue));
            }
        }
        _nRecords = input1.size();
        _dimensionValue = new String[input1.size()];

    }

    @Override
    public List<HeatMapCell> generateHeatMap(Map<String, Number> baseline, Map<String, Number> current)
    {
        int keyCount = 0;
        Set<Map.Entry<String,Number>> baselineSet =  baseline.entrySet();
        for (Map.Entry<String, Number> baselineEntry : baselineSet)  {
            String keyValue = baselineEntry.getKey();
            double s1 = baselineEntry.getValue().doubleValue();
            double s2 = current.get(keyValue).doubleValue();
            double[] s = new double[2];
            s[0] = s1;
            s[1] = s2;
            _snapshotDictionary.put(keyValue, s);
            _dimensionValue[keyCount] = keyValue;
            keyCount = keyCount + 1;
        }

        Stats baselineStats = getStats(baseline);
        Stats currentStats = getStats(current);
        double baselineSum = baselineStats.getSum();
        double currentSum = currentStats.getSum();
        List<HeatMapCell> cells = new ArrayList<HeatMapCell>();
        String resultString = updateRecords();
        for  (String entryString: resultString.split(",")) {
            if (_snapshotDictionary.containsKey(entryString)) {
                double[] valueList = _snapshotDictionary.get(entryString);
                baselineSum = baselineSum - valueList[0];
                currentSum = currentSum - valueList[1];
            }
        }
        double restRatio = currentSum / baselineSum - 1;
        HeatMapCell cell = new HeatMapCell("Rest",
                currentSum,
                baselineSum,
                restRatio,
                0.0,
                COLOR);
        cells.add(cell);

        for  (String entryString: resultString.split(",")) {
            if (_snapshotDictionary.containsKey(entryString)) {
                double[] valueList = _snapshotDictionary.get(entryString);
                double ratio = valueList[1] / valueList[0] - 1;
                String colorString = COLOR;
                if (ratio < 0) {
                    colorString = DOWNCOLOR;
                }
                if (ratio >= 0) {
                    colorString = UPCOLOR;
                }
                cell = new HeatMapCell(entryString,
                        valueList[1],
                        valueList[0],
                        ratio,
                        0.0,
                        colorString);
                cells.add(cell);
            }
        }
        return cells;
    }

    /**
     * loss function calculates the compression loss.
     * Say at time stamp 1, we have traffic v1, at time stamp 2, we
     * have traffic v2, and (time stamp 1 < time stamp 2). The function
     * measures the compression loss, if we use r*v1 to represent v2.
     * @param r: expected growth ratio, double.
     * @param v1: traffic at time stamp 1, double.
     * @param v2: traffic at time stamp 2, double.
     * @param knob: a parameter used in measure the compression loss, double.
     * @return: the information loss by representing v2 by v1 * r, double.
     */
    protected double loss(double r, double v1, double v2, double knob) {
        NormalDistribution GaussianDistObj = new NormalDistribution(r * v1, knob);
        return -Math.log(GaussianDistObj.density(v2));
    }

    /**
     * updateRecords outputs the possible components which help to explain the aggregate difference.
     * It creates a table to calculate the compression loss by DP algorithm.
     * @return the name of the underlying components which most explain the aggregate difference as a string.
     */
    public String updateRecords() {
        double r = GetInitialRatio();
        double lowR = r / 2;
        double upR = 2 * r;
        int steps = 100;
        double stepSize = (upR - lowR) / steps;
        double minCost = CreateOneTable(lowR)._tableCost;
        double initialR = lowR;
        double minR = lowR;

        for (int ii = 1; ii < steps; ii++) {
            double currentR = initialR + ii * stepSize;
            double currentCost = CreateOneTable(currentR)._tableCost;
            if (currentCost < minCost) {
                minCost = currentCost;
                minR = currentR;
            }
        }
        return CreateOneTable(minR)._records;
    }

    /**
     * GetInitialRatio outputs the initial growth ratio.
     * @return the growth ratio between two time stamps.
     */
    public double GetInitialRatio() {
        double valueT1 = 0.0;
        double valueT2 = 0.0;
        for (String key : _snapshotDictionary.keySet()) {
            double[] valueList = _snapshotDictionary.get(key);
            valueT1 = valueT1 + valueList[0];
            valueT2 = valueT2 + valueList[1];
        }
        return valueT2 / valueT1;
    }

    /**
     * CreateOneTable takes the given growth rate R, output an object of TableReturn, which contains
     * the compression cost using the R, and the components name which help to explain the difference.
     * @param r: the growth ratio.
     * @return an object of TableReturn.
     */
    public TableReturn CreateOneTable(double r) {
        int rowNum = _nRecords;
        int colNum = _maxRecords + 1;
        RealMatrix table = new Array2DRowRealMatrix(rowNum, colNum);
        String[][] recordTable = new String[rowNum][colNum];

        //update the knob value, i.e., variance
        double variance = 0.0;
        for (int ii = 0; ii < rowNum; ii++) {
            double[] valueList = _snapshotDictionary.get(_dimensionValue[ii]);
            variance += (valueList[1] - r * valueList[0]) * (valueList[1] - r * valueList[0]);
        }
        _knob = Math.sqrt(variance);

        //update the first column, i.e., just use one record.
        for (int row_index = 0; row_index < rowNum; row_index++) {
            double[] valueList = _snapshotDictionary.get(_dimensionValue[row_index]);
            if (row_index == 0) {
                table.setEntry(row_index, 0, loss(r, valueList[0], valueList[1], _knob));
            } else {
                table.setEntry(row_index, 0, table.getEntry(row_index - 1, 0) + loss(r, valueList[0], valueList[1], _knob));
            }
            recordTable[row_index][0] = "Rest";
        }
        //update the first row, i.e, using at least two records to cover the first element. No information loss.
        for (int col_index = 1; col_index < colNum; col_index++) {
            table.setEntry(0, col_index, 0);
            recordTable[0][col_index] = _dimensionValue[0];
        }
        // update the rest columns, i.e., use at least two record.
        for (int row_index = 1; row_index < rowNum; row_index++) {
            double[] valueList = _snapshotDictionary.get(_dimensionValue[row_index]);
            for (int col_index = 1; col_index < colNum; col_index++) {
                // D(T_(i+1), n, r) = min (D(T_i, n-1, r) + D(t_(i+1), 1, r), D(T_i, n, r) +D(t_(i+1), 0, r))
                double tmp1 = table.getEntry(row_index - 1, col_index - 1);
                double tmp2 = table.getEntry(row_index - 1, col_index) + loss(r, valueList[0], valueList[1], _knob);
                table.setEntry(row_index, col_index, Math.min(tmp1, tmp2));
                if (tmp1 < tmp2) {
                    recordTable[row_index][col_index] =
                            String.format("%s,%s", _dimensionValue[row_index], recordTable[row_index - 1][col_index - 1]);
                } else {
                    String tmp_str = recordTable[row_index - 1][col_index];
                    if (tmp_str.contains("Rest")) {
                        recordTable[row_index][col_index] = recordTable[row_index - 1][col_index];
                    } else
                        recordTable[row_index][col_index] = String.format("%s,%s", recordTable[row_index - 1][col_index], "Rest");
                }
            }
        }

        TableReturn rightLowerCell = new TableReturn();
        return rightLowerCell;
    }


    private static Stats getStats(Map<String, Number> metrics)
    {
        DescriptiveStatistics stats = new DescriptiveStatistics();

        for (Map.Entry<String, Number> entry : metrics.entrySet())
        {
            if (Double.NEGATIVE_INFINITY != entry.getValue().doubleValue())
            {
                stats.addValue(entry.getValue().doubleValue());
            }
        }

        double sum = stats.getSum();
        double average = stats.getMean();
        double variance = stats.getVariance();

        return new Stats(sum, average, variance);
    }

    private static class Stats
    {
        private final double sum;
        private final double average;
        private final double variance;

        Stats(double sum, double average, double variance)
        {
            this.sum = sum;
            this.average = average;
            this.variance = variance;
        }

        public double getSum()
        {
            return sum;
        }

        public double getAverage()
        {
            return average;
        }

        public double getVariance()
        {
            return variance;
        }
    }
}
