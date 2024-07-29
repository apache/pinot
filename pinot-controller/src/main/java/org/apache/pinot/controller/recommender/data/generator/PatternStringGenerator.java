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

import java.util.List;
import java.util.Map;
import org.apache.commons.configuration2.convert.PropertyConverter;


/**
 * TemplateStringGenerator produces series of strings by cycling through a predefined list of values, optionally with
 * a number of repetitions per value.
 *
 * Generator example:
 * <pre>
 *     values = [ "hello", "world" ]
 *     repetitions = 2
 *
 *     returns [ "hello", "hello", "world", "world", "hello", ... ]
 * </pre>
 *
 * Configuration examples:
 * <ul>
 *     <li>./pinot-tools/src/main/resources/generator/simpleWebsite_generator.json</li>
 *     <li>./pinot-tools/src/main/resources/generator/complexWebsite_generator.json</li>
 * </ul>
 */
public class PatternStringGenerator implements Generator {
  private final String[] _values;
  private final long _repetitions;

  private long _step;

  public PatternStringGenerator(Map<String, Object> templateConfig) {
    this(((List<String>) templateConfig.get("values")).toArray(new String[0]),
        PropertyConverter.toLong(templateConfig.getOrDefault("repetitions", 1)));
  }

  public PatternStringGenerator(String[] values, long repetitions) {
    _values = values;
    _repetitions = repetitions;
  }

  @Override
  public void init() {
    // left blank
  }

  @Override
  public Object next() {
    return _values[(int) (_step++ / _repetitions) % _values.length];
  }
}
