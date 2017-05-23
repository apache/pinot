package com.linkedin.thirdeye.anomalydetection.model.merge;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;
import com.linkedin.thirdeye.anomalydetection.model.prediction.ExpectedTimeSeriesPredictionModel;
import com.linkedin.thirdeye.anomalydetection.model.prediction.PredictionModel;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimplePercentageMergeModel extends AbstractMergeModel {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimplePercentageMergeModel.class);

  private static final String DEFAULT_MESSAGE_TEMPLATE = "change : %.2f %%, currentVal : %.2f, baseLineVal : %.2f, score : %.2f";

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
    String mainMetric =
        anomalyDetectionContext.getAnomalyDetectionFunction().getSpec().getTopicMetric();

    PredictionModel predictionModel = anomalyDetectionContext.getTrainedPredictionModel(mainMetric);
    if (!(predictionModel instanceof ExpectedTimeSeriesPredictionModel)) {
      LOGGER.error("SimplePercentageMergeModel expects an ExpectedTimeSeriesPredictionModel but the trained model is not one.");
      return;
    }

    ExpectedTimeSeriesPredictionModel expectedTimeSeriesPredictionModel =
        (ExpectedTimeSeriesPredictionModel) predictionModel;

    TimeSeries expectedTimeSeries = expectedTimeSeriesPredictionModel.getExpectedTimeSeries();
    long expectedStartTime = expectedTimeSeries.getTimeSeriesInterval().getStartMillis();

    TimeSeries observedTimeSeries = anomalyDetectionContext.getTransformedCurrent(mainMetric);
    long observedStartTime = observedTimeSeries.getTimeSeriesInterval().getStartMillis();

    double avgCurrent = 0d;
    double avgBaseline = 0d;
    int count = 0;
    Interval anomalyInterval =
        new Interval(anomalyToUpdated.getStartTime(), anomalyToUpdated.getEndTime());

    for (long observedTimestamp : observedTimeSeries.timestampSet()) {
      if (anomalyInterval.contains(observedTimestamp)) {
        long offset = observedTimestamp - observedStartTime;
        long expectedTimestamp = expectedStartTime + offset;

        if (expectedTimeSeries.hasTimestamp(expectedTimestamp)) {
          avgCurrent += observedTimeSeries.get(observedTimestamp);
          avgBaseline += expectedTimeSeries.get(expectedTimestamp);
          ++count;
        }
      }
    }

    double weight = 0d;
    if (count != 0 && avgBaseline != 0d) {
      weight = (avgCurrent - avgBaseline) / avgBaseline;
      avgCurrent /= count;
      avgBaseline /= count;
    } else {
      weight = 0d;
    }

    // Average score of raw anomalies
    List<RawAnomalyResultDTO> rawAnomalyResultDTOs = anomalyToUpdated.getAnomalyResults();
    double score = 0d;
    if (CollectionUtils.isNotEmpty(rawAnomalyResultDTOs)) {
      for (RawAnomalyResultDTO rawAnomaly : rawAnomalyResultDTOs) {
        score += rawAnomaly.getScore();
      }
      score /= rawAnomalyResultDTOs.size();
    } else {
      score = anomalyToUpdated.getScore();
    }

    anomalyToUpdated.setWeight(weight);
    anomalyToUpdated.setScore(score);
    anomalyToUpdated.setAvgCurrentVal(avgCurrent);
    anomalyToUpdated.setAvgBaselineVal(avgBaseline);
    anomalyToUpdated.setMessage(
        String.format(DEFAULT_MESSAGE_TEMPLATE, weight * 100, avgCurrent, avgBaseline, score));
  }
}
