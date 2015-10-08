package com.linkedin.thirdeye.lib.scanstatistics;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Range;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * Online scan statistics implementation.
 */
public class ScanStatistics {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScanStatistics.class);

  private static final Random RANDOM = new Random();

	private final int _numSimulation;
	private final int _minWindowLength;
	private final int _maxWindowLength;
	private final int _minIncrement;
	private final boolean _bootstrap;
	private final double _pValue;
	private final Pattern _pattern;
	private final double _notEqualEpsilon;

	/**
	 * The direction of the hypothesis test
	 */
	public enum Pattern {
	  UP, DOWN, NOTEQUAL,
	}

	public ScanStatistics(int numSimulation, int minWindowLength, int maxWindowLength, double pValue, Pattern pattern,
												int minIncrement, boolean bootstrap)  {
		this(numSimulation, minWindowLength, maxWindowLength, pValue, pattern, minIncrement, bootstrap, 0);
	}

	public ScanStatistics(int numSimulation, int minWindowLength, int maxWindowLength, double pValue, Pattern pattern,
	    int minIncrement, boolean bootstrap, double notEqualEpsilon)  {
		_numSimulation = numSimulation;
		_minWindowLength = minWindowLength;
		_maxWindowLength = maxWindowLength;
		_minIncrement = minIncrement;
		_pValue = pValue;
		_pattern = pattern;
		_bootstrap = bootstrap;
		_notEqualEpsilon = notEqualEpsilon;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("_numSimulation", _numSimulation)
				.add("_minWindowLength", _minWindowLength)
				.add("_maxWindowLength", _maxWindowLength)
				.add("_minIncrement", _minIncrement)
				.add("_pValue", _pValue)
				.add("_pattern", _pattern)
				.add("_bootstrap", _bootstrap)
				.toString();
	}

	 /**
   * This function finds the given interval in the monitoring window of which the maximum likelihood
   * values above the simulated quantile.
   *
   * @return
   *  An interval with starting point and ending point, or null if none surpass the threshold
   */
  public Range<Integer> getInterval(double[] trainingData, double[] monitoringData) {

    OnlineNormalStatistics trainDataDs = new OnlineNormalStatistics(trainingData);
    NormalDistribution trainDataNormal = new NormalDistribution(trainDataDs.getMean(),
        Math.sqrt(trainDataDs.getPopulationVariance()));
		LOGGER.info("Training data mean={}, stdev={}", trainDataNormal.getMean(), trainDataNormal.getStandardDeviation());

    ScanIntervalIterator scanWindowIterator = new ScanIntervalIterator(
        0, monitoringData.length, _minWindowLength, _maxWindowLength, _minIncrement);
    MaxInterval realDataInterval = generateMaxLikelihood(scanWindowIterator, trainingData, monitoringData, trainDataDs);
    if (realDataInterval.getInterval() == null) {
			return null;
    }
		LOGGER.info("Generated realDataInterval {}", realDataInterval);

    int numExceeded = 0;
    int exceededCountThreshold = (int) (_pValue * _numSimulation);

    // simulation buffer
    double[] simulationBuffer = new double[monitoringData.length];
    for (int ii = 0; ii < _numSimulation; ii++) {
      if (_bootstrap) {
        simulateBootstrapInPlace(simulationBuffer, trainingData);
      } else {
        simulateGaussuanInPlace(simulationBuffer, trainDataNormal);
      }

      ScanIntervalIterator simulationScanWindowIterator = new ScanIntervalIterator(
          0, monitoringData.length, _minWindowLength, _maxWindowLength, _minIncrement);
      MaxInterval simulationResult = generateMaxLikelihood(simulationScanWindowIterator, trainingData, simulationBuffer,
          trainDataDs);

			LOGGER.debug("simulation ({}) {} (numExceeded={}) : {}",
					_bootstrap ? "bootstrap" : "gaussian", ii, numExceeded, simulationResult);

      if (simulationResult.getInterval() != null
          && realDataInterval.getMaxLikelihood() < simulationResult.getMaxLikelihood())
      {
        numExceeded++;
        if (numExceeded >= exceededCountThreshold) {
          // early stopping condition
          break;
        }
      }
    }

    LOGGER.info("real data interval: {} (percentile {})", realDataInterval,
        1 - (numExceeded / (double)_numSimulation));

    if (numExceeded < exceededCountThreshold) {
      return realDataInterval.getInterval();
    } else {
      return null;
    }
  }

  /**
   * @param simulationData
   *  The array that will be modified in place.
   * @param dist
   *  Normal distribution from which values are drawn.
   */
  private void simulateGaussuanInPlace(double[] simulationData, NormalDistribution dist) {
    for (int i = 0; i < simulationData.length; i++) {
      simulationData[i] = dist.sample();
    }
  }

  /**
   * @param simulationData
   *  The array that will be modified in place.
   * @param trainData
   *  Array from which samples are drawn.
   */
  private void simulateBootstrapInPlace(double[] simulationData, double[] trainData) {
    for (int i = 0; i < simulationData.length; i++) {
      simulationData[i] = trainData[RANDOM.nextInt(trainData.length)];
    }
  }

	/**
	 * This function generates necessary statistics for a given period of a time series.
	 *
	 * @param range
	 *  The interval considered as 'in'
	 * @param inDs
	 *  The descriptive statistics to register 'in' values to
	 * @param outDs
	 *  The descriptive statistics to register 'out' values to
	 */
	private void getTimeSeriesStats(Range<Integer> range, double[] data, OnlineNormalStatistics inDs,
	    OnlineNormalStatistics outDs) {
	  // TODO should be able to leverage sliding windows to do this more efficiently
	  for (int i = 0; i < data.length; i++) {
	    if (range.contains(i)) {
	      inDs.addValue(data[i]);
	    } else {
	      outDs.addValue(data[i]);
	    }
	  }
	}

	/**
	 * This function finds the scanning window which has the maximum likelihood values defined by the
	 * scanning hypothesis.
	 *
	 * @param scanWindowIterator
	 *   Iterator from which to get scan windows.
	 * @param trainDataDs
	 *   OnlineNormalStatistics for the train data
	 * @return
	 *   The interval which gives the maximum likelihood values.
	 */
	private MaxInterval generateMaxLikelihood(ScanIntervalIterator scanWindowIterator, double[] trainingData,
	    double[] monitoringData, OnlineNormalStatistics trainDataDs)
	{
		double maxValue = Double.NEGATIVE_INFINITY;
		Range<Integer> maxInterval = null;

		OnlineNormalStatistics dsAll = trainDataDs.copy();
    for (double d : monitoringData) {
      dsAll.addValue(d);
    }

    double allVar = dsAll.getPopulationVariance();
    double N = trainingData.length + monitoringData.length;

    /* The first three terms are shared */
    double sharedTerms = (dsAll.getSumSqDev() / (2 * allVar)) + (N * Math.log(Math.sqrt(allVar))) - (0.5 * N);

		while(scanWindowIterator.hasNext())
		{
		  Range<Integer> currentScanWindow = scanWindowIterator.next();

		  // initialize descriptive statistics
		  OnlineNormalStatistics inDs = new OnlineNormalStatistics();
	    OnlineNormalStatistics outDs = trainDataDs.copy();

		  getTimeSeriesStats(currentScanWindow, monitoringData, inDs, outDs);

			double inMean = inDs.getMean();
			double outMean = outDs.getMean();

			double sharedVar = (outDs.getSumSqDev() + inDs.getSumSqDev()) / N;
			double currentValue = sharedTerms - (N * Math.log(Math.sqrt(sharedVar)));
			boolean matchesPattern = false;
			switch (_pattern)
			{
			  case UP: {
			    matchesPattern = inMean > outMean;
			    break;
			  }
			  case DOWN: {
			    matchesPattern = inMean < outMean;
			    break;
			  }
			  case NOTEQUAL: {
					matchesPattern = Math.abs(inMean - outMean) > _notEqualEpsilon * outMean;
			    break;
			  }
			}

			if (currentValue > maxValue && matchesPattern) {
			  maxValue = currentValue;
			  maxInterval = currentScanWindow;
			}
		}

		MaxInterval maxDataInterval = new MaxInterval(maxValue, maxInterval);
		return maxDataInterval;
	}

	/**
	 * Test on known anomalies
	 *
	 * @param args
	 * @throws IOException
	 */
//	public static void main(String[] args) throws IOException {
//    String[] lines = ResourceUtils.getResourceAsString("timeseries.csv").split("\n");
//    int numData = lines.length;
//    long[] timestamps = new long[numData];
//    double[] series = new double[numData];
//    for (int i = 0; i < numData; i++) {
//      timestamps[i] = i;
//      String value = lines[i].split(",")[1];
//      if (value.equals("NA")) {
//        series[i] = 0;
//      } else {
//        series[i] = Double.valueOf(value);
//      }
//    }
//
//    long start = System.currentTimeMillis();
//    double[] data = removeSeasonality(timestamps, series, 168);
//
//
//    int split = 800;
//    double[] train = Arrays.copyOfRange(data, 0, split);
//    double[] monitor = Arrays.copyOfRange(data, split, data.length);
//
//    ScanStatistics scanStatistics = new ScanStatistics(
//        1000,
//        1,
//        100000,
//        0.05,
//        Pattern.DOWN,
//        1,
//        false);
//
//    Range<Integer> anomaly = scanStatistics.getInterval(train, monitor);
//    Range<Integer> anomalyOffset = Range.closedOpen(anomaly.lowerEndpoint() + split, anomaly.upperEndpoint() + split);
//
//    System.out.println("N : " + data.length);
//    System.out.println("Split : " + split);
//    System.out.println("Anomaly : " + anomalyOffset);
//    System.out.println("Runtime: " + TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start) + " seconds");
//	}
//
//	private static double[] removeSeasonality(long[] timestamps, double[] series, int seasonality) {
//	  STLDecomposition.Config config = new STLDecomposition.Config();
//    config.setNumberOfObservations(seasonality);
//    config.setNumberOfInnerLoopPasses(1);
//    config.setNumberOfRobustnessIterations(5);
//    config.setLowPassFilterBandwidth(0.5);
//    config.setTrendComponentBandwidth(0.5);
//    config.setPeriodic(true);
//    STLDecomposition stl = new STLDecomposition(config);
//
//    STLResult res = stl.decompose(timestamps, series);
//
//    double[] trend = res.getTrend();
//    double[] remainder = res.getRemainder();
//    double[] seasonalityRemoved = new double[trend.length];
//    for (int i = 0; i < trend.length; i++) {
//      seasonalityRemoved[i] = trend[i] + remainder[i];
//    }
//    return seasonalityRemoved;
//	}
}






