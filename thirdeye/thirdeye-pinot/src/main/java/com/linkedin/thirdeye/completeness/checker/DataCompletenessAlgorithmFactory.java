/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
