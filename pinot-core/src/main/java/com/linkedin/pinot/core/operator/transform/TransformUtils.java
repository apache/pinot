/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.operator.transform;

import com.linkedin.pinot.core.operator.transform.function.AdditionTransform;
import com.linkedin.pinot.core.operator.transform.function.DateTimeConversionTransform;
import com.linkedin.pinot.core.operator.transform.function.DivisionTransform;
import com.linkedin.pinot.core.operator.transform.function.MultiplicationTransform;
import com.linkedin.pinot.core.operator.transform.function.SubtractionTransform;
import com.linkedin.pinot.core.operator.transform.function.TimeConversionTransform;


/**
 * Utility class for Transform functions.
 */
public class TransformUtils {

  // Private constructor to prevent instantiation of the class
  private TransformUtils() {

  }

  // Array of all built-in Transform functions. This must be updated each time
  // a new transform is added.
  private static final String[] BUILT_IN_TRANSFORMS = {
      AdditionTransform.class.getName(),
      SubtractionTransform.class.getName(),
      MultiplicationTransform.class.getName(),
      DivisionTransform.class.getName(),
      TimeConversionTransform.class.getName(),
      DateTimeConversionTransform.class.getName()
  };

  /**
   * Utility function to get all built-in transform function.
   *
   * @return Array of class names of built-in transform functions.
   */
  public static String[] getBuiltInTransform() {
    return BUILT_IN_TRANSFORMS;
  }
}
