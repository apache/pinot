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

import org.apache.commons.math3.distribution.LogNormalDistribution;

import java.util.Map;

/**
 * TemplateSpikeGenerator produces a series of log-normal spikes with log-normal arrival times, with optional smoothing.
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
public class TemplateSpikeGenerator implements Generator {
    private final double baseline;
    private final double smoothing;

    private final LogNormalDistribution arrivalGenerator;
    private final LogNormalDistribution magnitudeGenerator;

    private long step = -1;

    private long nextArrival;
    private double lastValue;

    public TemplateSpikeGenerator(Map<String, Object> templateConfig) {
        this(toDouble(templateConfig.get("baseline"), 0),
                toDouble(templateConfig.get("arrivalMean"), 2),
                toDouble(templateConfig.get("arrivalSigma"), 1),
                toDouble(templateConfig.get("magnitudeMean"), 2),
                toDouble(templateConfig.get("magnitudeSigma"), 1),
                toDouble(templateConfig.get("smoothing"), 0));
    }

    public TemplateSpikeGenerator(double baseline, double arrivalMean, double arrivalSigma, double magnitudeMean, double magnitudeSigma, double smoothing) {
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

    private static double toDouble(Object obj, double defaultValue) {
        if (obj == null) {
            return defaultValue;
        }
        return Double.valueOf(obj.toString());
    }

    public static void main(String[] args) {
        TemplateSpikeGenerator gen = new TemplateSpikeGenerator(15, 2, 1, 3, 1, 0.25);
        for (int i = 0; i < 100; i++) {
            System.out.println(gen.next());
        }
    }
}
