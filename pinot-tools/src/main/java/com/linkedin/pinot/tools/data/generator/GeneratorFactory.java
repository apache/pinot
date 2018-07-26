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
package com.linkedin.pinot.tools.data.generator;

import com.linkedin.pinot.common.data.FieldSpec.DataType;


/**
 * Sep 13, 2014
 */

public class GeneratorFactory {

  public static Class getGeneratorFor(DataType type) {

    if (type == DataType.STRING) {
      return StringGenerator.class;
    }

    return NumberGenerator.class;
  }

  public static Generator getGeneratorFor(DataType type, int cardinality) {
    if (type == DataType.STRING) {
      return new StringGenerator(cardinality);
    }

    return new NumberGenerator(cardinality, type, false);
  }

  public static Generator getGeneratorFor(DataType dataType, int start, int end) {
    Generator generator;

    switch (dataType) {
      case INT:
        generator = new RangeIntGenerator(start, end);
        break;

      case LONG:
        generator = new RangeLongGenerator(start, end);
        break;

      case FLOAT:
        generator = new RangeFloatGenerator(start, end);
        break;

      case DOUBLE:
        generator = new RangeDoubleGenerator(start, end);
        break;

      default:
        throw new RuntimeException("Invalid datatype");

    }
    return generator;
  }
}
