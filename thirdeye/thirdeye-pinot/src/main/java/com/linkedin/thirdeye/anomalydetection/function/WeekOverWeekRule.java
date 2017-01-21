package com.linkedin.thirdeye.anomalydetection.function;

import com.linkedin.thirdeye.anomalydetection.model.data.SeasonalDataModel;
import com.linkedin.thirdeye.anomalydetection.model.detection.SimpleThresholdDetectionModel;
import com.linkedin.thirdeye.anomalydetection.model.prediction.SeasonalAveragePredictionModel;
import com.linkedin.thirdeye.anomalydetection.model.transform.MovingAverageSmoothingFunction;
import com.linkedin.thirdeye.anomalydetection.model.transform.TransformationFunction;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;

public class WeekOverWeekRule extends AbstractAnomalyDetectionFunction {
  public static final String BASELINE = "baseline";
  public static final String ENABLE_SMOOTHING = "enableSmoothing";

  @Override
  public void init(AnomalyFunctionDTO spec) throws Exception {
    super.init(spec);
    this.init(this.properties);
  }

  public void init(Properties properties) {
    this.properties = properties;

    String baselineProp = this.properties.getProperty(BASELINE);
    if (StringUtils.isNotBlank(baselineProp)) {
      this.initPropertiesForDataModel(baselineProp);
    }
    dataModel = new SeasonalDataModel();
    dataModel.init(this.properties);

    if (this.properties.containsKey(ENABLE_SMOOTHING)) {
      TransformationFunction movingAverageSoothingFunction = new MovingAverageSmoothingFunction();
      movingAverageSoothingFunction.init(this.properties);
      currentTimeSeriesTransformationChain.add(movingAverageSoothingFunction);
      baselineTimeSeriesTransformationChain.add(movingAverageSoothingFunction);
    }

    predictionModel = new SeasonalAveragePredictionModel();
    predictionModel.init(this.properties);

    detectionModel = new SimpleThresholdDetectionModel();
    detectionModel.init(this.properties);
  }

  /**
   * The strings in the format of "w/w" is defined to be backward compatible with old anomaly
   * detection framework. These strings should be deprecated after the migration.
   *
   * TODO: Replace the w/w strings with ENUM.
   *
   * @param baselineProp The human readable string of baseline property for setting up
   *                     SEASONAL_PERIOD and SEASONAL_SIZE.
   */
  private void initPropertiesForDataModel(String baselineProp) {
    // The basic settings for w/w
    this.properties.setProperty(SeasonalDataModel.SEASONAL_PERIOD, "1");
    this.properties.setProperty(SeasonalDataModel.SEASONAL_SIZE, "7");
    this.properties.setProperty(SeasonalDataModel.SEASONAL_UNIT, "DAYS");
    // Change the setting for different w/w types
    if ("w/2w".equals(baselineProp)) {
      this.properties.setProperty(SeasonalDataModel.SEASONAL_SIZE, "14");
    } else if ("w/3w".equals(baselineProp)) {
      this.properties.setProperty(SeasonalDataModel.SEASONAL_SIZE, "21");
    } else if ("w/4w".equals(baselineProp)) {
      this.properties.setProperty(SeasonalDataModel.SEASONAL_SIZE, "28");
    } else if ("w/2wAvg".equals(baselineProp)) {
      this.properties.setProperty(SeasonalDataModel.SEASONAL_PERIOD, "2");
    } else if ("w/3wAvg".equals(baselineProp)) {
      this.properties.setProperty(SeasonalDataModel.SEASONAL_PERIOD, "3");
    } else if ("w/4wAvg".equals(baselineProp)) {
      this.properties.setProperty(SeasonalDataModel.SEASONAL_PERIOD, "4");
    }
  }
}
