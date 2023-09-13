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
package org.apache.pinot.controller.recommender.data.generator;

import java.util.Map;
import org.apache.commons.configuration2.convert.PropertyConverter;


/**
 * PatternSequenceGenerator produces a series of sequentially increasing (decreasing) numbers, optionally with a fixed
 * number of repetitions per values. This pattern is typical for monotonically increasing series such as timestamps.
 *
 * Generator example:
 * <pre>
 *     start = -10
 *     stepsize = 3
 *     repetitions = 2
 *
 *     returns [ -10, -10, -7, -7, -4, -4, -1, -1, 2, 2, ... ]
 * </pre>
 *
 * Configuration examples:
 * <ul>
 *     <li>./pinot-tools/src/main/resources/generator/simpleWebsite_generator.json</li>
 *     <li>./pinot-tools/src/main/resources/generator/complexWebsite_generator.json</li>
 * </ul>
 */
public class PatternSequenceGenerator implements Generator {
  private final long _start;
  private final long _stepsize;
  private final long _repetitions;

  private long _step = -1;

  public PatternSequenceGenerator(Map<String, Object> templateConfig) {
    this(PropertyConverter.toLong(templateConfig.getOrDefault("start", 0)),
        PropertyConverter.toLong(templateConfig.getOrDefault("stepsize", 1)),
        PropertyConverter.toLong(templateConfig.getOrDefault("repetitions", 1)));
  }

  public PatternSequenceGenerator(long start, long stepsize, long repetitions) {
    _start = start;
    _stepsize = stepsize;
    _repetitions = repetitions;
  }

  @Override
  public void init() {
    // left blank
  }

  @Override
  public Object next() {
    _step++;
    return _start + (_step / _repetitions) * _stepsize;
  }
}
