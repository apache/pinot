package com.linkedin.thirdeye.anomaly.lib.scanstatistics;
import java.util.HashSet;
import java.lang.Integer;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.util.Pair;

import com.linkedin.thirdeye.anomaly.lib.scanstatistics.MaxInterval;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: jjchen
 * Date: 8/25/15
 * Time: 9:42 PM
 * To change this template use File | Settings | File Templates.
 */
public class ScanStatistics {
	public int _numSimulation;
	public int _minWindowLength;
	public int _minIncrement;
	public double _pValue;
	public Pattern _pattern;
	public enum Pattern {
	    UP, DOWN,NOTEQUAL,
	}

	public ScanStatistics(int numSimulation, int minWindowLength, double pValue, Pattern pattern, int minIncrement)  {
		_numSimulation = numSimulation;
		_minWindowLength = minWindowLength;
		_minIncrement = minIncrement;
		_pValue = pValue;
		_pattern = pattern;

	}
	/**
	 * Generate all possible scan windows for the monitoring window.
	 * @param start_index: the starting point of the monitoring window.
	 * @param end_index: the end point of the monitoring window.
	 * @return a set of scanning windows represented by (start point, end point).
	 * (TODO): add_maxWindowLength
	 */

	public HashSet<Pair<Integer, Integer>> generateScanWindows(int start_index, int end_index) {
		if (end_index - start_index < _minWindowLength) {
			return null;
		}
		HashSet<Pair<Integer, Integer>> scanWindows = new HashSet<Pair<Integer, Integer>> ();
		for (int idx = start_index;  idx < end_index-_minWindowLength; idx=idx+_minIncrement)  {
			for (int idx2 = idx+_minWindowLength; idx2 < end_index; idx2=idx2+_minIncrement)  {
				scanWindows.add(new Pair<Integer, Integer>(idx, idx2));
			}
		}
		return scanWindows;

	}
	/**
	 * This function generates necessary statistics for a given period of a time series.
	 * @param a_scanwindow: a pair of integers, the first/second one indicates the starting/ending of the scanning window.
	 * @param data: the data sequence which contains the data in the training window and the data in monitoring window.
	 * @param isIn: a boolean flag indicates the statistics is generated for the data inside/outside a_scanwindow. 
	 * @return a descriptive statistics object.
	 */
	public DescriptiveStatistics getTimeSeriesStats(Pair<Integer, Integer> a_scanwindow, double[] data, boolean isIn) {
		int left_index = a_scanwindow.getKey();
		int right_index = a_scanwindow.getValue();
		if (left_index < 0 | right_index > data.length) {
			return null;
		}
		if (isIn) {
			double[] subTimeSeries = new double[right_index-left_index];
			for (int ii=left_index; ii < right_index; ii++)   {
				subTimeSeries[ii] = data[ii];
			} 
			DescriptiveStatistics inObj = new DescriptiveStatistics(subTimeSeries);
			return inObj;          
		} else {
			int current_timeSeries_len = data.length - (right_index-left_index);
			double[] subTimeSeries = new double[current_timeSeries_len];
			for (int ii=0; ii < left_index; ii++)   {
				subTimeSeries[ii] = data[ii];
			}
			for (int ii=right_index; ii<data.length; ii++) {
				subTimeSeries[ii] = data[ii];
			}    
			DescriptiveStatistics outObj = new DescriptiveStatistics(subTimeSeries);
			return outObj;
		} 
	}
	/**
	 * This function finds the scanning window which has the maximum likelihood values defined by the 
	 * scanning hypothesis. 
	 * @param ScanWindowSet: a set of scanning windows.
	 * @param data: a data vector contains training data and simulated data for monitoring window.
	 * @return the interval which gives the maximum likelihood values.
	 */

	public MaxInterval generateMaxLikelihood(HashSet<Pair<Integer, Integer>> ScanWindowSet, double[] data) {
		if (ScanWindowSet == null)
			return null;
		double  maxValue = Double.NEGATIVE_INFINITY;
		Pair<Integer, Integer>  maxInterval = null;
		for (Pair<Integer, Integer> a_scanwindow : ScanWindowSet)  {
			DescriptiveStatistics inObj =  getTimeSeriesStats(a_scanwindow, data, true);
			DescriptiveStatistics outObj = getTimeSeriesStats(a_scanwindow, data, false);
			if (inObj != null & outObj != null )  {
				DescriptiveStatistics allObj = new DescriptiveStatistics(data);
				double inMean = inObj.getMean();
				double outMean = outObj.getMean();
				double allMean = allObj.getMean();
				double allVar = allObj.getVariance();
				double sharedVar = 0;
				int total_len = data.length;
				for (int ii=0; ii < data.length; ii++){
					if (ii >= a_scanwindow.getKey() & ii< a_scanwindow.getValue()) {
						sharedVar +=  (data[ii] - inMean) * (data[ii] - inMean);
					}  else {
						sharedVar +=  (data[ii] - outMean) * (data[ii] - outMean);
					}
				}
				sharedVar =  sharedVar / (double) total_len;
				double currentValue = (double)total_len * Math.log(Math.sqrt(allVar)) - (double)total_len /2.0 - (double)total_len * Math.log(Math.sqrt(sharedVar));
				for (int ii =0; ii<total_len; ii++) {
					currentValue = (data[ii] - allMean) * (data[ii] - allMean) / (2.0 * allVar);
				}
				int pattern_indicator;
				if (_pattern == Pattern.UP) {
					pattern_indicator = (inMean > outMean) ? 1 : 0;
				} else if (_pattern == Pattern.DOWN) {
					pattern_indicator = (inMean < outMean) ? 1 : 0;
				} else {
					pattern_indicator =  (inMean != outMean) ? 1 : 0;
				}

				if (currentValue > maxValue & pattern_indicator == 1) {
					maxValue = currentValue;
					maxInterval = a_scanwindow;
				}
			}
		}
		MaxInterval maxDataInterval = new MaxInterval(maxValue, maxInterval);
		return maxDataInterval;
	}
	/**
	 * This version we include the online simulation version.  Each run will 
	 * simulate and generate the threshold.
	 * @param data: training data.
	 * @param nrow: number of simulations.
	 * @param ncol: length(training data) + length(monitoring data)
	 * @return a double array which contains #nrow of rows. Each row, row[1:length(data)] is the training data,
	 * row[length(data)+1: ncol] contains the simulated values based on Gaussian assumptions.
	 */
	public double[][] simulation(double[] data, int nrow, int ncol){
		double[][] simulationMatrix = new double[nrow][ncol];
		DescriptiveStatistics dataObj = new DescriptiveStatistics(data);
		double data_mean = dataObj.getMean();
		double data_var = dataObj.getVariance();
		for (int row_idx = 0; row_idx < nrow; row_idx++) {
			for (int col_idx=0; col_idx < ncol; col_idx++)  {
				if (col_idx < data.length) {
					simulationMatrix[row_idx][col_idx] = data[col_idx];
				} else {
					Random random_rv = new Random();
					simulationMatrix[row_idx][col_idx] = data_mean + Math.sqrt(data_var) * random_rv.nextGaussian();
				}
			}
		}
		return simulationMatrix;
	}
	/**
	 * This function generates the given quantile for an array.
	 * @param an array of maxLikelihood values for different scanning windows.
	 * @return a Percentile obj.
	 */
	public double computeQuantile(double[] maxLikelihoodArray) {
		Percentile Percentile_obj = new Percentile();
		Percentile_obj.setData(maxLikelihoodArray);
		return Percentile_obj.evaluate(1 -_pValue);
	}
	/**
	 * This function finds the given interval in the monitoring window of which the maximum likelihood 
	 * values above the simulated quantile.
	 * @param start_index: starting point of the monitoring window.
	 * @param end_index: ending point of the monitoring window.
	 * @param data: a data array which contains training and monitoring.
	 * @return an interval with starting point and ending point.
	 */

	public Pair<Integer, Integer> getInterval(int start_index, int end_index, double[] data) {
		// (TODO): AssertionError() check start_index < end_index
		// (TODO): Assert length(data) > end_index
		HashSet<Pair<Integer, Integer>> ScanWindowsSet =   generateScanWindows(start_index, end_index);
		double[] training_data = new double[start_index];
		for (int ii = 0; ii<start_index; ii++){
			training_data[ii] = data[ii];
		}

		double[][] SimulationMatrix = simulation(training_data, _numSimulation, data.length);

		double[] maxLikelihoodArray = new double[_numSimulation];

		for  (int ii = 0; ii < _numSimulation; ii++) {

			maxLikelihoodArray[ii] = generateMaxLikelihood(ScanWindowsSet, SimulationMatrix[ii])._maxLikelihood;
		}
		double threshold = computeQuantile(maxLikelihoodArray);

		MaxInterval realDataInterval = generateMaxLikelihood(ScanWindowsSet, data);
		if (realDataInterval._maxLikelihood > threshold) {
			return realDataInterval._interval;
		}  else {
			return null;
		}
	}

}






