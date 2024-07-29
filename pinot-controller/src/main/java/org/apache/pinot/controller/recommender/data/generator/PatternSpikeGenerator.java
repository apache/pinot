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
import org.apache.commons.math3.distribution.AbstractRealDistribution;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.commons.math3.random.Well19937c;


/**
 * PatternSpikeGenerator produces a series of log-normal spikes with log-normal arrival times, with optional smoothing.
 * This pattern is typical for rare even spikes, such as error counts. The generated values are sampled
 * non-deterministically.
 *
 * Generator example:
 * <pre>
 *     baseline = 0
 *     arrivalMean = ?
 *     magnitudeMean = ?
 *
 *     returns [ 0, 0, 0, 0, 0, 0, 47, 15, 2, 1, 0, 0, ... ]
 * </pre>
 *
 * Configuration examples:
 * <ul>
 *     <li>./pinot-tools/src/main/resources/generator/simpleWebsite_generator.json</li>
 *     <li>./pinot-tools/src/main/resources/generator/complexWebsite_generator.json</li>
 * </ul>
 */
public class PatternSpikeGenerator implements Generator {
  private final double _baseline;
  private final double _smoothing;

  private final AbstractRealDistribution _arrivalGenerator;
  private final AbstractRealDistribution _magnitudeGenerator;

  private long _step = -1;

  private long _nextArrival;
  private double _lastValue;

  enum DistributionType {
    LOGNORMAL, EXPONENTIAL, UNIFORM, FIXED
  }

  public PatternSpikeGenerator(Map<String, Object> templateConfig) {
    this(PropertyConverter.toDouble(templateConfig.getOrDefault("baseline", 0)),
        DistributionType.valueOf(templateConfig.getOrDefault("arrivalType", "lognormal").toString().toUpperCase()),
        PropertyConverter.toDouble(templateConfig.getOrDefault("arrivalMean", 2)),
        PropertyConverter.toDouble(templateConfig.getOrDefault("arrivalSigma", 1)),
        DistributionType.valueOf(templateConfig.getOrDefault("magnitudeType", "lognormal").toString().toUpperCase()),
        PropertyConverter.toDouble(templateConfig.getOrDefault("magnitudeMean", 2)),
        PropertyConverter.toDouble(templateConfig.getOrDefault("magnitudeSigma", 1)),
        PropertyConverter.toDouble(templateConfig.getOrDefault("smoothing", 0)),
        PropertyConverter.toInteger(templateConfig.getOrDefault("seed", 0)));
  }

  public PatternSpikeGenerator(double baseline, DistributionType arrivalType, double arrivalMean, double arrivalSigma,
      DistributionType magnitudeType, double magnitudeMean, double magnitudeSigma, double smoothing, int seed) {
    _baseline = baseline;
    _smoothing = smoothing;

    _arrivalGenerator = makeDist(arrivalType, arrivalMean, arrivalSigma, seed);
    _magnitudeGenerator = makeDist(magnitudeType, magnitudeMean, magnitudeSigma, seed);

    _nextArrival = (long) _arrivalGenerator.sample();
    _lastValue = baseline;
  }

  private static AbstractRealDistribution makeDist(DistributionType type, double mean, double sigma, int seed) {
    switch (type) {
      case LOGNORMAL:
        return new LogNormalDistribution(new Well19937c(seed), mean, sigma, 1.0E-9D);
      case EXPONENTIAL:
        return new ExponentialDistribution(new Well19937c(seed), mean, 1.0E-9D);
      case UNIFORM:
        return new UniformRealDistribution(new Well19937c(seed), mean - sigma, mean + sigma);
      case FIXED:
        return new UniformRealDistribution(new Well19937c(seed), mean, mean + 1.0E-9D);
      default:
        throw new IllegalArgumentException(String.format("Unsupported distribution type '%s", type));
    }
  }

  @Override
  public void init() {
    // left blank
  }

  @Override
  public Object next() {
    _step++;

    if (_step < _nextArrival) {
      _lastValue = (1 - _smoothing) * _baseline + _smoothing * _lastValue;
      return (long) _lastValue;
    }

    _nextArrival += (long) _arrivalGenerator.sample();
    _lastValue = _baseline + _magnitudeGenerator.sample();
    return (long) _lastValue;
  }
}
