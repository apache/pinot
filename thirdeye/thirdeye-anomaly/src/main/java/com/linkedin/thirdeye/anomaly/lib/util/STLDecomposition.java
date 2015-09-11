package com.linkedin.thirdeye.anomaly.lib.util;

import java.util.ArrayList;

import org.apache.commons.math3.analysis.interpolation.LoessInterpolator;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/**
 * Implementation of STL: A Seasonal-Trend Decomposition Procedure based on Loess.
 *
 * <p>
 *   Robert B. Cleveland et al., "STL: A Seasonal-Trend Decomposition Procedure based on Loess,"
 *   in Journal of Official Statistics Vol. 6 No. 1, 1990, pp. 3-73
 * </p>
 *
 * @author Greg Brandt (gbrandt@linkedin.com)
 * @author Jieying Chen (jjchen@linkedin.com)
 * @author James Hong (jhong@linkedin.com)
 */
public class STLDecomposition {
  private static final int LOESS_ROBUSTNESS_ITERATIONS = 4; // same as R implementation

  private final Config config;

  /**
   * Constructs a configuration of STL function that can de-trend data.
   *
   * <p>
   *   n.b. The Java Loess implementation only does  linear local polynomial
   *   regression, but R supports linear (degree=1), quadratic (degree=2), and
   *   a strange degree=0 option.
   * </p>
   *
   * <p>
   *   Also, the Java Loess implementation accepts "bandwidth", the fraction of source points closest
   *   to the current point, as opposed to integral values.
   * </p>
   */
  public STLDecomposition(Config config) {
    config.check();
    this.config = config;
  }

  public static class Config {
    /** The number of observations in each cycle of the seasonal component, n_p */
    private int numberOfObservations;
    /** The number of passes through the inner loop, n_i */
    private int numberOfInnerLoopPasses = 1;
    /** The number of robustness iterations of the outer loop, n_o */
    private int numberOfRobustnessIterations = 1;
    /** The smoothing parameter for the low pass filter, like n_l */
    private double lowPassFilterBandwidth = 0.25;
    /** The smoothing parameter for the trend component, like n_t */
    private double trendComponentBandwidth = 0.25;
    /** The smoothing parameter for the seasonal component, like n_s */
    private double seasonalComponentBandwidth = 0.25;
    /** Whether the series is periodic, if this is true, then seasonalComponentBandwidth is ignored. */
    private boolean periodic = false;

    public Config() {}

    public int getNumberOfObservations() {
      return numberOfObservations;
    }

    public void setNumberOfObservations(int numberOfObservations) {
      this.numberOfObservations = numberOfObservations;
    }

    public int getNumberOfInnerLoopPasses() {
      return numberOfInnerLoopPasses;
    }

    public void setNumberOfInnerLoopPasses(int numberOfInnerLoopPasses) {
      this.numberOfInnerLoopPasses = numberOfInnerLoopPasses;
    }

    public int getNumberOfRobustnessIterations() {
      return numberOfRobustnessIterations;
    }

    public void setNumberOfRobustnessIterations(int numberOfRobustnessIterations) {
      this.numberOfRobustnessIterations = numberOfRobustnessIterations;
    }

    public double getLowPassFilterBandwidth() {
      return lowPassFilterBandwidth;
    }

    public void setLowPassFilterBandwidth(double lowPassFilterBandwidth) {
      this.lowPassFilterBandwidth = lowPassFilterBandwidth;
    }

    public double getTrendComponentBandwidth() {
      return trendComponentBandwidth;
    }

    public void setTrendComponentBandwidth(double trendComponentBandwidth) {
      this.trendComponentBandwidth = trendComponentBandwidth;
    }

    public double getSeasonalComponentBandwidth() {
      return seasonalComponentBandwidth;
    }

    public void setSeasonalComponentBandwidth(double seasonalComponentBandwidth) {
      this.seasonalComponentBandwidth = seasonalComponentBandwidth;
    }

    public boolean isPeriodic() {
      return periodic;
    }

    public void setPeriodic(boolean periodic) {
      this.periodic = periodic;
    }


    public void check() {
      checkPeriodicity(numberOfObservations);

      // TODO: Check n_t, needs to be n_t >= 1.5 * n_p / (1 - 1.5/n_s)
    }

    private int checkPeriodicity(int numberOfObservations) {
      if (numberOfObservations < 2) {
        throw new IllegalArgumentException("Periodicity (numberOfObservations) must be >= 2");
      }
      return numberOfObservations;
    }
  }

  /**
   * The STL decomposition of a time series.
   *
   * <p>
   *   getData() == getTrend() + getSeasonal() + getRemainder()
   * </p>
   */
  public class STLResult {
    private final long[] times;
    private final double[] series;
    private final double[] trend;
    private final double[] seasonal;
    private final double[] remainder;

    public STLResult(long[] times, double[] series, double[] trend, double[] seasonal, double[] remainder) {
      this.times = times;
      this.series = series;
      this.trend = trend;
      this.seasonal = seasonal;
      this.remainder = remainder;
    }

    public long[] getTimes() {
      return times;
    }

    public double[] getSeries() {
      return series;
    }

    public double[] getTrend() {
      return trend;
    }

    public double[] getSeasonal() {
      return seasonal;
    }

    public double[] getRemainder() {
      return remainder;
    }
  }

  /**
   * @param times
   * @param series
   * @return
   */
  public STLResult decompose(long[] times, double[] series) {
    double[] trend = new double[series.length];
    double[] seasonal = new double[series.length];
    double[] remainder = new double[series.length];
    double[] robustness = null;

    for (int l = 0; l < config.getNumberOfRobustnessIterations(); l++) {
      for (int k = 0; k < config.getNumberOfInnerLoopPasses(); k++) {
        // Step 1: Detrending
        double[] detrend = new double[series.length];
        for (int i = 0; i < series.length; i++) {
          detrend[i] = series[i] - trend[i];
        }

        // Get cycle sub-series with padding on either side
        int numberOfObservations = config.getNumberOfObservations();

        ArrayList<double[]> cycleSubseries = new ArrayList<>(numberOfObservations);
        ArrayList<double[]> cycleTimes = new ArrayList<>(numberOfObservations);
        ArrayList<double[]> cycleRobustnessWeights = new ArrayList<double[]>(numberOfObservations);

        for (int i = 0; i < numberOfObservations; i++) {
          int subseriesLength = series.length / numberOfObservations;
          subseriesLength += (i < series.length % numberOfObservations) ? 1 : 0;
          double[] subseriesValues = new double[subseriesLength];
          double[] subseriesTimes = new double[subseriesLength];
          double[] subseriesRobustnessWeights = null;
          if (robustness != null) {
            subseriesRobustnessWeights = new double[subseriesLength];
          }

          for (int cycleIdx = 0; cycleIdx < subseriesLength; cycleIdx++) {
            subseriesValues[cycleIdx] = detrend[cycleIdx * numberOfObservations + i];
            subseriesTimes[cycleIdx] = times[cycleIdx * numberOfObservations + i];
            if (subseriesRobustnessWeights != null) {
              subseriesRobustnessWeights[cycleIdx] = robustness[cycleIdx * numberOfObservations + i];
            }
          }

          cycleSubseries.add(subseriesValues);
          cycleTimes.add(subseriesTimes);
          cycleRobustnessWeights.add(subseriesRobustnessWeights);
        }

        // Step 2: Cycle-subseries Smoothing
        for (int i = 0; i < cycleSubseries.size(); i++) {
          double[] smoothed;
          if (config.isPeriodic()) {
            smoothed = weightedMeanSmooth(cycleSubseries.get(i), cycleRobustnessWeights.get(i));
          } else {
            smoothed = loessSmooth(cycleTimes.get(i), cycleSubseries.get(i), config.getSeasonalComponentBandwidth(),
                cycleRobustnessWeights.get(i));
          }
          cycleSubseries.set(i, smoothed);
        }

        // Combine smoothed series into one
        double[] combinedSmoothed = new double[series.length];
        for (int i = 0; i < cycleSubseries.size(); i++) {
          double[] subseriesValues = cycleSubseries.get(i);
          for (int cycleIdx = 0; cycleIdx < subseriesValues.length; cycleIdx++) {
            combinedSmoothed[numberOfObservations * cycleIdx + i] = subseriesValues[cycleIdx];
          }
        }

        // Step 3: Low-Pass Filtering of Smoothed Cycle-Subseries
        double[] filtered = lowPassFilter(combinedSmoothed, robustness);

        // Step 4: Detrending of Smoothed Cycle-Subseries
        for (int i = 0; i < seasonal.length; i++) {
          seasonal[i] = combinedSmoothed[i] - filtered[i];
        }

        // Step 5: Deseasonalizing
        for (int i = 0; i < series.length; i++) {
          trend[i] = series[i] - seasonal[i];
        }

        // Step 6: Trend Smoothing
        trend = loessSmooth(trend, config.getTrendComponentBandwidth(), robustness);
      }

      // --- Now in outer loop ---

      // Calculate remainder
      for (int i = 0; i < series.length; i++) {
        remainder[i] = series[i] - trend[i] - seasonal[i];
      }

      // Calculate robustness weights using remainder
      robustness = robustnessWeights(remainder);
    }

    return new STLResult(times, series, trend, seasonal, remainder);
  }

  private double[] robustnessWeights(double[] remainder) {
    // Compute "h" = 6 median(|R_v|)
    double[] absRemainder = new double[remainder.length];
    for (int i = 0; i < remainder.length; i++) {
      absRemainder[i] = Math.abs(remainder[i]);
    }
    DescriptiveStatistics stats = new DescriptiveStatistics(absRemainder);
    double h = 6 * stats.getPercentile(50);

    // Compute robustness weights
    double[] robustness = new double[remainder.length];
    for (int i = 0; i < remainder.length; i++) {
      robustness[i] = biSquareWeight(absRemainder[i] / h);
    }

    return robustness;
  }

  private double biSquareWeight(double u) {
    if (u < 0) {
      throw new IllegalArgumentException("Invalid u, must be >= 0: " + u);
    } else if (u < 1) {
      return Math.pow(1 - Math.pow(u, 2), 2);
    } else {
      return 0;
    }
  }

  private double[] lowPassFilter(double[] series, double[] weights) {
    // Apply moving average of length n_p, twice
    series = movingAverage(series, config.getNumberOfObservations());
    series = movingAverage(series, config.getNumberOfObservations());
    // Apply moving average of length 3
    series = movingAverage(series, 3);
    // Loess smoothing with d = 1, q = n_l
    series = loessSmooth(series, config.getLowPassFilterBandwidth(), weights);
    return series;
  }

  /**
   * @param series
   * @param bandwidth
   * @param weights
   *  The weights to use for smoothing, if null, equal weights are assumed
   * @return
   *  Smoothed series
   */
  private double[] loessSmooth(double[] series, double bandwidth, double[] weights) {
    double[] times = new double[series.length];
    for (int i = 0; i < series.length; i++) {
      times[i] = i;
    }
    return loessSmooth(times, series, bandwidth, weights);
  }

  /**
   * @param times
   * @param series
   * @param bandwidth
   * @param weights
   *  The weights to use for smoothing, if null, equal weights are assumed
   * @return
   *  Smoothed series
   */
  private double[] loessSmooth(double[] times, double[] series, double bandwidth, double[] weights) {
    if (weights == null) {
      return new LoessInterpolator(bandwidth, LOESS_ROBUSTNESS_ITERATIONS).smooth(times, series);
    } else {
      return new LoessInterpolator(bandwidth, LOESS_ROBUSTNESS_ITERATIONS).smooth(times, series, weights);
    }
  }

  private double[] weightedMeanSmooth(double[] series, double[] weights) {
    double[] smoothed = new double[series.length];
    double mean = 0;
    double sumOfWeights = 0;
    for (int i = 0; i < series.length; i++) {
      double weight = (weights != null) ? weights[i] : 1; // equal weights if none specified
      mean += weight * series[i];;
      sumOfWeights += weight;
    }
    mean /= sumOfWeights;
    for (int i = 0; i < series.length; i++) {
      smoothed[i] = mean;
    }
    return smoothed;
  }

  private double[] movingAverage(double[] series, int window) {
    double[] movingAverage = new double[series.length];
    DescriptiveStatistics ds = new DescriptiveStatistics(window);
    for (int i = 0; i < series.length; i++) {
      ds.addValue(series[i]);
      movingAverage[i] = ds.getMean();
    }
    return movingAverage;
  }

}
