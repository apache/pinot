package com.linkedin.thirdeye.tools.migrate;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.util.StringUtils;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class MigraterTestUtils {
  public static final String DATA = "data";
  public static final String TESTING_PREPROCESS = "testingPreprocessors";
  public static final String TRAINING_PREPROCESS = "trainingPreprocessors";
  public static final String TRAINING = "training";
  public static final String DETECTION = "detection";

  public static AnomalyFunctionDTO getAnomalyFunctionDTO(String functionType, int bucketSize, TimeUnit bucketUnit,
      Properties properties) {
    AnomalyFunctionDTO functionDTO = new AnomalyFunctionDTO();
    functionDTO.setFunctionName("test anomaly function");
    functionDTO.setType(functionType);
    functionDTO.setBucketSize(bucketSize);
    functionDTO.setBucketUnit(bucketUnit);
    functionDTO.setProperties(StringUtils.encodeCompactedProperties(properties));
    return functionDTO;
  }
}
