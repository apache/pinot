package com.linkedin.thirdeye.anomaly.lib.fanomaly;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.optim.MaxEval;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;
import org.apache.commons.math3.optim.univariate.BrentOptimizer;
import org.apache.commons.math3.optim.univariate.SearchInterval;
import org.apache.commons.math3.optim.univariate.UnivariateObjectiveFunction;
import org.apache.commons.math3.optim.univariate.UnivariatePointValuePair;
import org.jblas.DoubleMatrix;


public class StateSpaceAnomalyDetector extends AnomalyDetector {

  private int seasonal;
  private int order;
  private int numStates;
  private int outputStates;
  private double r;

  private final static Logger LOGGER = Logger.getLogger(StateSpaceAnomalyDetector.class.getName());

  public StateSpaceAnomalyDetector(long trainStartInput, long trainEndInput, int stepsAheadInput,
      long timeGranularityInput, Set<Long> omitTimestampsInput, int seasonalInput, int orderInput,
      int numStatesInput, int outputStatesInput, double rInput)
  {
    super(trainStartInput, trainEndInput, stepsAheadInput, timeGranularityInput, omitTimestampsInput);
    seasonal = seasonalInput;
    order = orderInput;
    numStates = numStatesInput;
    outputStates = outputStatesInput;
    r = rInput;
    LOGGER.warning("Rvalue: " + r);
  }

  private double EstimateTrainingMean(DoubleMatrix[] processTrainingTimeSeries) {
    double estimateMean = 0;
    double count = 0;
    for (int ii = 0; ii < seasonal + 1; ii++)
    {
      if (processTrainingTimeSeries[ii] != null)
      {
        count = count + 1;
        estimateMean = estimateMean + processTrainingTimeSeries[ii].get(0, 0);
      }
    }
    estimateMean = estimateMean / count;
    return estimateMean;
  }

  private double EstimateTrainingVariance(DoubleMatrix[] processTrainingTimeSeries) {
    double estimateVariance = 0;
    double count = 0;
    for (int ii = 0; ii < seasonal + 1; ii++)
    {
      if (processTrainingTimeSeries[ii] != null)
      {
        count = count + 1;
        estimateVariance = estimateVariance
            + processTrainingTimeSeries[ii].get(0, 0) * processTrainingTimeSeries[ii].get(0, 0);
      }
    }
    estimateVariance = estimateVariance / count;
    return estimateVariance;
  }

  public Map<Long, FanomalyDataPoint> ConstantTrainingSequenceCase(DoubleMatrix[] inputTimeSeries,
      long[] inputTimeStamps) throws AnomalyException, Exception {

    DoubleMatrix[] trainingTimeSeries = getTrainingData(inputTimeSeries, inputTimeStamps);
    DoubleMatrix[] processTrainingTimeSeries = RemoveTimeStamps(trainingTimeSeries, inputTimeStamps);
    double estimateMean = EstimateTrainingMean(processTrainingTimeSeries);
    Map<Long, FanomalyDataPoint> output = new HashMap<Long, FanomalyDataPoint>();

    for (int ii = 0; ii < processTrainingTimeSeries.length; ii++ )
    {
      if (processTrainingTimeSeries[ii] == null)
      {
        //pValue[ii] = Double.NaN;
        output.put(inputTimeStamps[ii], new FanomalyDataPoint(Double.NaN, Double.NaN, Double.NaN, 0.0,
            inputTimeStamps[ii], ii));
      }
      else
      {
        if (processTrainingTimeSeries[ii].get(0, 0) == estimateMean)
        {
          //pValue[ii]=1.0;
          output.put(inputTimeStamps[ii], new FanomalyDataPoint(estimateMean, estimateMean, 1.0, 0.0,
              inputTimeStamps[ii], ii));
          /*LOGGER.warning(String.format("%d,%f,%f,%f, %f,%d\n", ii,
              output.get(inputTimeStamps[ii]).actualValue,
              output.get(inputTimeStamps[ii]).predictedValue,
              output.get(inputTimeStamps[ii]).pValue,
              output.get(inputTimeStamps[ii]).stdError,
              output.get(inputTimeStamps[ii]).predictedDate));*/
        }
      }
    }

    return output;
  }

  public StateSpaceModel EstimateStateSpaceModel(DoubleMatrix[] inputTimeSeries, long[] inputTimeStamps)
      throws AnomalyException, Exception {

    DoubleMatrix[] trainingTimeSeries = getTrainingData(inputTimeSeries, inputTimeStamps);
    DoubleMatrix[] processTrainingTimeSeries = RemoveTimeStamps(trainingTimeSeries, inputTimeStamps);

    if (processTrainingTimeSeries.length <= seasonal) {
      throw new AnomalyException("timestamp not long enough for one season.");
    }

    // Kalman here
    DoubleMatrix GG = new DoubleMatrix(numStates, numStates);
    DoubleMatrix FF = new DoubleMatrix(outputStates, numStates);

    for (int ii = 0; ii < order; ii++) {
      GG.put(0, ii, 1);
    }

    if (order == 2) {
      GG.put(1, 1, 1);
    }

    if (seasonal > 0) {
      for (int ii = order; ii < numStates; ii++) {
        GG.put(order, ii, -1);
      }
      for (int ii = order + 1; ii < numStates; ii++) {
        GG.put(ii, (ii-1), 1);
      }
    }

    FF.put(0, 0, 1);
    if (seasonal > 0) {
      FF.put(0, 1, 1);
    }

    DoubleMatrix m0 = new DoubleMatrix(numStates, 1);
    DoubleMatrix c0 = new DoubleMatrix(numStates, numStates);
    double estimateVariance = EstimateTrainingVariance(processTrainingTimeSeries);
    double estimateMean = EstimateTrainingMean(processTrainingTimeSeries);
    LOGGER.warning("estimated input data variance");
    LOGGER.warning(String.format("%f", estimateVariance));

    if (estimateVariance == 0) {
      // if all constant training sequence, return null object here and
      // no state space model is called.
      throw new AnomalyException("no output");
    }

    for (int ii = 0; ii < numStates; ii++) {
      c0.put(ii, ii, estimateVariance);
    }
    m0.put(0, 0, estimateMean);

    BrentOptimizer optimizer = new BrentOptimizer(1e-6, 1e-12);
    UnivariatePointValuePair solution = optimizer.optimize(
        new UnivariateObjectiveFunction(new StateSpaceUnivariateObj(GG, FF, r, m0,c0, processTrainingTimeSeries)),
        new MaxEval(100),
        GoalType.MAXIMIZE,
        new SearchInterval(0.0001, estimateVariance));
    double EstimatedStateNoise = solution.getPoint();

    //construct observation noise matrix and state noise matrix there
    DoubleMatrix StateNoiseMatrix = DoubleMatrix.eye(numStates).muli(EstimatedStateNoise);
    DoubleMatrix ObservationNoiseMatrix = DoubleMatrix.eye(outputStates).muli(EstimatedStateNoise * r);

    StateSpaceModel Subject = new StateSpaceModel(GG, FF, StateNoiseMatrix,
        ObservationNoiseMatrix, m0, c0, processTrainingTimeSeries);
    return Subject;
  }

  public Map<Long, FanomalyDataPoint> DetectAnomaly(double[] inputData, long[] inputTimeStamps, long offset)
      throws Exception {

    DoubleMatrix[] inputTimeSeries =  new DoubleMatrix[inputData.length];

    for (int ii = 0; ii < inputData.length; ii++)
    {
      if (Double.isNaN(inputData[ii]))
      {
        inputTimeSeries[ii] = null;
      } else {
        inputTimeSeries[ii] = new DoubleMatrix(new double[] {inputData[ii]});
      }
    }

    StateSpaceModel Subject =  EstimateStateSpaceModel(inputTimeSeries, inputTimeStamps);
    if (Subject == null) {
      Map<Long, FanomalyDataPoint> output = ConstantTrainingSequenceCase(inputTimeSeries, inputTimeStamps);
      return output;
    }

    // output
    Subject.CalculatePrediction(stepsAhead);
    DoubleMatrix[] estimatedMean = Subject.GetEstimatedMeans();
    DoubleMatrix[] estimatedCovariance = Subject.GetEstimatedCovariances();
    NormalDistribution pCal =  new NormalDistribution(0, 1);

    //LOGGER.warning(predictedMean[1].get(0, 0));
    //LOGGER.warning(predictedCovariance[1].get(0, 0));

    // output P value here
    if (stepsAhead == -1)
    {
      // assuming one dimension here (todo: extend to multi input)
      DoubleMatrix[] trainingSequence = Subject.GetTrainingSequence();

      //double[] pValue = new double[trainingSequence.length];
      Map<Long, FanomalyDataPoint> output = new HashMap<Long, FanomalyDataPoint>();
      for (int ii = 0; ii < trainingSequence.length; ii++)
      {
        double MeanTmp = estimatedMean[ii].get(0, 0);
        double VarianceTmp = estimatedCovariance[ii].get(0, 0);
//        double MeanTmpState =Subject.GetEstimatedStateMeans()[ii].get(0, 0);
//        double VarianceTmpState = Subject.GetEstimatedStateCovariances()[ii].get(0, 0);

        //pValue[ii] = new DoubleMatrix(outputStates, outputStates);
        if (trainingSequence[ii] == null)
        {
          //pValue[ii] = Double.NaN;
          output.put(inputTimeStamps[ii], new FanomalyDataPoint(MeanTmp, Double.NaN, Double.NaN,Math.sqrt(VarianceTmp),
              inputTimeStamps[ii], ii));
        }
        else
        {
          double Actual = trainingSequence[ii].get(0,0);
          double aPvalue = 1-pCal.cumulativeProbability(Math.abs(Actual-MeanTmp) / Math.sqrt(VarianceTmp));
          output.put(inputTimeStamps[ii], new FanomalyDataPoint(MeanTmp, Actual, aPvalue, Math.sqrt(VarianceTmp),
              inputTimeStamps[ii], ii));

          //pValue[ii].put(0, 0, aPvalue);
          /*LOGGER.warning(String.format("%d,%f,%f,%f,%f, %f,%f\n",
                           ii+1, Actual,
                           MeanTmp, VarianceTmp, aPvalue,MeanTmpState,
                           VarianceTmpState));*/
        }
      }
      return output;
    }
    else // stepsAhead != -1
    {
      DoubleMatrix[] predictionSequence = getPredictionData(inputTimeSeries, inputTimeStamps);
      if (predictionSequence == null) {
        throw new AnomalyException("no output");
      }

      //double[] pValue = new double[predictionSequence.length];
      Map<Long, FanomalyDataPoint> output = new HashMap<Long, FanomalyDataPoint>();
      long monitorStart = trainEnd + offset;
      for (int ii = 0; ii < predictionSequence.length; ii++)
      {
        double MeanTmp = estimatedMean[ii].get(0, 0);
        double VarianceTmp = estimatedCovariance[ii].get(0, 0);
        double Actual = predictionSequence[ii].get(0,0);
        double aPvalue = 1 - pCal.cumulativeProbability(Math.abs(Actual-MeanTmp) / Math.sqrt(VarianceTmp));

        //pValue[ii] = aPvalue;
        // todo here to fix the date range
        output.put(monitorStart + ii * offset , new FanomalyDataPoint(MeanTmp, Actual, aPvalue,
            Math.sqrt(VarianceTmp), monitorStart + ii * offset, ii));
      }
      return output;
    }
  }
}
