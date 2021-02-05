/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.tools.data.generator;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.pinot.spi.data.FieldSpec.DataType;

import java.util.Map;


/**
 * Sep 13, 2014
 */

public class GeneratorFactory {
  public static Generator getGeneratorFor(DataType type, int cardinality) {
    if (type == DataType.STRING) {
      return new StringGenerator(cardinality);
    }

    return new NumberGenerator(cardinality, type, false);
  }

  public static Generator getGeneratorFor(DataType dataType, int start, int end) {
    switch (dataType) {
      case INT:
        return new RangeIntGenerator(start, end);
      case LONG:
        return new RangeLongGenerator(start, end);
      case FLOAT:
        return new RangeFloatGenerator(start, end);
      case DOUBLE:
        return new RangeDoubleGenerator(start, end);
      case BYTES:
        return new RangeBytesGenerator(start, end);
      default:
        throw new RuntimeException(String.format("Invalid datatype '%s'", dataType));
    }
  }

  public static Generator getGeneratorFor(PatternType patternType, Map<String, Object> templateConfig) {
    switch (patternType) {
      case SEASONAL:
        return new PatternSeasonalGenerator(templateConfig);
      case SPIKE:
        return new PatternSpikeGenerator(templateConfig);
      case SEQUENCE:
        return new PatternSequenceGenerator(templateConfig);
      case STRING:
        return new PatternStringGenerator(templateConfig);
      case MIXTURE:
        return new PatternMixtureGenerator(templateConfig);
      default:
        throw new RuntimeException(String.format("Invalid template '%s'", patternType));
    }
  }
}
