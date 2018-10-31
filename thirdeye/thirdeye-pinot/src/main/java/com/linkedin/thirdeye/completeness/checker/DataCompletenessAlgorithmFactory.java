package com.linkedin.thirdeye.completeness.checker;


import java.lang.reflect.Constructor;

public class DataCompletenessAlgorithmFactory {


  public static DataCompletenessAlgorithm getDataCompletenessAlgorithmFromClass(String algorithmClass) {
    DataCompletenessAlgorithm dataCompletenessAlgorithm = null;
    try {
      Constructor<?> constructor = Class.forName(algorithmClass).getConstructor();
      dataCompletenessAlgorithm = (DataCompletenessAlgorithm) constructor.newInstance();
    } catch (Exception e) {
      throw new IllegalArgumentException("Data completeness checker could not instantiate class " + algorithmClass);
    }
    return dataCompletenessAlgorithm;
  }

}
