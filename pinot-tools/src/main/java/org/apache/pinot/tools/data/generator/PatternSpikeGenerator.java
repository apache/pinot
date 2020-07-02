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

import org.apache.commons.configuration.PropertyConverter;
import org.apache.commons.math3.distribution.LogNormalDistribution;

import java.util.Map;

/**
 * PatternSpikeGenerator produces a series of log-normal spikes with log-normal arrival times, with optional smoothing.
 * This pattern is typical for rare even spikes, such as error counts. The generated values are sampled non-deterministically.
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
    private final double baseline;
    private final double smoothing;

    private final LogNormalDistribution arrivalGenerator;
    private final LogNormalDistribution magnitudeGenerator;

    private long step = -1;

    private long nextArrival;
    private double lastValue;

    public PatternSpikeGenerator(Map<String, Object> templateConfig) {
        this(PropertyConverter.toDouble(templateConfig.getOrDefault("baseline", 0)),
                PropertyConverter.toDouble(templateConfig.getOrDefault("arrivalMean", 2)),
                PropertyConverter.toDouble(templateConfig.getOrDefault("arrivalSigma", 1)),
                PropertyConverter.toDouble(templateConfig.getOrDefault("magnitudeMean", 2)),
                PropertyConverter.toDouble(templateConfig.getOrDefault("magnitudeSigma", 1)),
                PropertyConverter.toDouble(templateConfig.getOrDefault("smoothing", 0)));
    }

    public PatternSpikeGenerator(double baseline, double arrivalMean, double arrivalSigma, double magnitudeMean, double magnitudeSigma, double smoothing) {
        this.baseline = baseline;
        this.smoothing = smoothing;

        this.arrivalGenerator = new LogNormalDistribution(arrivalMean, arrivalSigma);
        this.magnitudeGenerator = new LogNormalDistribution(magnitudeMean, magnitudeSigma);

        this.nextArrival = (long) arrivalGenerator.sample();
        this.lastValue = baseline;
    }

    @Override
    public void init() {
        // left blank
    }

    @Override
    public Object next() {
        step++;

        if (step < nextArrival) {
            lastValue = (1 - smoothing) * baseline + smoothing * lastValue;
            return (long) lastValue;
        }

        nextArrival += (long) arrivalGenerator.sample();
        lastValue = baseline + this.magnitudeGenerator.sample();
        return (long) lastValue;
    }
}
