package com.linkedin.thirdeye.anomalydetection.model.merge;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;
import com.linkedin.thirdeye.anomalydetection.model.prediction.ExpectedTimeSeriesPredictionModel;
import com.linkedin.thirdeye.anomalydetection.model.prediction.PredictionModel;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimplePercentageMergeModel extends AbstractMergeModel {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimplePercentageMergeModel.class);

  private double avgObserved;
  private double avgExpected;

  /**
   * The weight of the merged anomaly is calculated by this equation:
   *     weight = (avg. observed value) / (avg. expected value) - 1;
   *
   * Note that the values of the holes in the time series are not included in the computation.
   * Considering the observed and expected time series:
   *    observed:  1 2 x 4 x 6
   *    expected:  1 x x 4 5 6
   * The values that are included in the computation are those at slots 1, 4, and 6.
   *
   * @param anomalyDetectionContext the context that provided a trained
   *                                ExpectedTimeSeriesPredictionModel for computing the weight.
   *                                Moreover, the data range of the time series should equals the
   *                                range of anomaly to be updated.
   *
   * @param anomalyToUpdated the anomaly of which the information is updated.
   */
  @Override
  public void update(AnomalyDetectionContext anomalyDetectionContext,
      MergedAnomalyResultDTO anomalyToUpdated) {
    PredictionModel predictionModel = anomalyDetectionContext.getTrainedPredictionModel();
    if (!(predictionModel instanceof ExpectedTimeSeriesPredictionModel)) {
      LOGGER.error("SimplePercentageMergeModel expects an ExpectedTimeSeriesPredictionModel but the trained model is not one.");
    }

    ExpectedTimeSeriesPredictionModel expectedTimeSeriesPredictionModel =
        (ExpectedTimeSeriesPredictionModel) predictionModel;

    TimeSeries expectedTimeSeries = expectedTimeSeriesPredictionModel.getExpectedTimeSeries();
    long expectedStartTime = expectedTimeSeries.getTimeSeriesInterval().getStartMillis();

    TimeSeries observedTimeSeries = anomalyDetectionContext.getCurrent();
    long observedStartTime = observedTimeSeries.getTimeSeriesInterval().getStartMillis();

    avgObserved = 0d;
    avgExpected = 0d;
    int count = 0;
    for (long observedTimestamp : observedTimeSeries.timestampSet()) {
      long offset = observedTimestamp - observedStartTime;
      long expectedTimestamp = expectedStartTime + offset;

      if (expectedTimeSeries.hasTimestamp(expectedTimestamp)) {
        avgObserved += observedTimeSeries.get(observedTimestamp);
        avgExpected += expectedTimeSeries.get(expectedTimestamp);
        ++count;
      }
    }

    if (count != 0 && avgExpected != 0d) {
      weight = (avgObserved - avgExpected) / avgExpected;
      avgObserved /= count;
      avgExpected /= count;
    } else {
      weight = 0d;
    }

    // Average score of raw anomalies
    List<RawAnomalyResultDTO> rawAnomalyResultDTOs = anomalyDetectionContext.getRawAnomalies();
    score = 0d;
    if (CollectionUtils.isNotEmpty(rawAnomalyResultDTOs)) {
      for (RawAnomalyResultDTO rawAnomaly : rawAnomalyResultDTOs) {
        score += rawAnomaly.getScore();
      }
      score /= rawAnomalyResultDTOs.size();
    } else {
      score = anomalyToUpdated.getScore();
    }
  }

  public double getAvgObserved() {
    return avgObserved;
  }

  public double getAvgExpected() {
    return avgExpected;
  }
}
