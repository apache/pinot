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

import java.util.*;
import java.util.stream.Collectors;


/**
 * PatternMixtureGenerator enables combination of multiple Generators in alternating and additive patterns, including
 * nested mixture models. This is typically used to generate similar (but not exact copies of) time series for different
 * dimension values of series or to simulate anomalous behavior in a otherwise regular time series.
 *
 * Generator example:
 * <pre>
 *     generator bins = [
 *       [ { type = "SEASONAL", mean = 10, ... } ],
 *       [ { type = "SEASONAL", mean = 30, ... }, { type = "SPIKE", arrivalMean = 2, ... } ],
 *       [ { type = "SEASONAL", mean = 50, ... } ],
 *     ]
 *
 *     returns [ 10, 30, 50, 11, 29, 52, 10, 114, 51, 9, 64, 50, 10, 35, 49, ... ]
 * </pre>
 *
 * Configuration examples:
 * <ul>
 *     <li>./pinot-tools/src/main/resources/generator/complexWebsite_generator.json</li>
 * </ul>
 */
public class PatternMixtureGenerator implements Generator {
  private final List<List<Generator>> generatorBins;

  private long step = -1;

  public PatternMixtureGenerator(Map<String, Object> templateConfig) {
    this(toGeneratorBins((List<List<Map<String, Object>>>) templateConfig.get("generatorBins"),
        (Map<String, Object>) templateConfig.get("defaults")));
  }

  public PatternMixtureGenerator(List<List<Generator>> generatorBins) {
    this.generatorBins = generatorBins;
  }

  private static List<List<Generator>> toGeneratorBins(List<List<Map<String, Object>>> templateConfigBins,
      Map<String, Object> defaults) {
    final List<List<Map<String, Object>>> safeBins =
        templateConfigBins == null ? new ArrayList<>() : templateConfigBins;
    final Map<String, Object> safeDefaults = defaults == null ? new HashMap<>() : defaults;

    return safeBins.stream().map(conf -> toGenerators(conf, safeDefaults)).collect(Collectors.toList());
  }

  private static List<Generator> toGenerators(List<Map<String, Object>> templateConfigs, Map<String, Object> defaults) {
    return templateConfigs.stream().map(conf -> {
      Map<String, Object> augmentedConf = new HashMap<>(defaults);
      augmentedConf.putAll(conf);
      return toGenerator(augmentedConf);
    }).collect(Collectors.toList());
  }

  private static Generator toGenerator(Map<String, Object> templateConfig) {
    PatternType type = PatternType.valueOf(templateConfig.get("type").toString());
    return GeneratorFactory.getGeneratorFor(type, templateConfig);
  }

  @Override
  public void init() {
    // left blank
  }

  @Override
  public Object next() {
    step++;
    int bin = (int) step % generatorBins.size();
    long output = 0;
    for (Generator gen : generatorBins.get(bin)) {
      output += (Long) gen.next();
    }
    return output;
  }
}
