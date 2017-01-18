package com.linkedin.thirdeye.anomalydetection.function;

import com.linkedin.thirdeye.anomalydetection.model.data.SeasonalDataModel;
import com.linkedin.thirdeye.anomalydetection.model.prediction.SeasonalAveragePredictionModel;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.commons.lang3.StringUtils;

public class WeekOverWeekRule extends AbstractAnomalyDetectionFunction {
  public static final String BASELINE = "baseline";

  @Override
  public void init(AnomalyFunctionDTO spec) throws Exception {
    super.init(spec);

    String baselineProp = this.properties.getProperty(BASELINE);
    if (StringUtils.isNotBlank(baselineProp)) {
      this.initPropertiesForDataModel(baselineProp);
    }
    dataModel = new SeasonalDataModel();
    dataModel.init(this.properties);

    if (this.properties.contains("enableSmoothing")) {
      //TODO: currentTimeSeriesTransformationChain.add(AverageSmoothingTransformationFunction);
      // baselineTimeSeriesTransformationChain.add(AverageSmoothingTransformationFunction);
    }

    predictionModel = new SeasonalAveragePredictionModel();

    // TODO: detectionModel = new SomeDetectionModel();
  }

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
