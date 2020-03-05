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

import org.apache.commons.math3.distribution.NormalDistribution;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * TemplateSeasonalGenerator generates sinus wave patterns with a linear trend, gaussian noise, and cyclically repeating
 * scaling factors. These patterns are typical for di-urnal usage patterns such as clicks and impressions of a website.
 *
 * Generator example:
 * <pre>
 *     mean = 10
 *     sigma = 1
 *     wavelength = 4
 *     amplitude = 10
 *     scaling factors = [ 0.5, 1.0, 1.0, 1.0, 1.0, 0.5 ] // e.g. high weekdays, low week ends
 *
 *     returns [ 5, 10, 12, 7, 11, 18, 21, 6, 2, 7, 12, 20, 21, 13, ... ]
 * </pre>
 *
 * Configuration examples:
 * <ul>
 *     <li>./pinot-tools/src/main/resources/generator/simpleWebsite_generator.json</li>
 *     <li>./pinot-tools/src/main/resources/generator/complexWebsite_generator.json</li>
 * </ul>
 */
public class TemplateSeasonalGenerator implements Generator {
    private final double trend;
    private final double wavelength;
    private final double amplitude;
    private final double[] scalingFactors;

    private final NormalDistribution generator;

    private long step = -1;

    public TemplateSeasonalGenerator(Map<String, Object> templateConfig) {
        this(toDouble(templateConfig.get("mean"), 0),
                toDouble(templateConfig.get("sigma"), 0),
                toDouble(templateConfig.get("trend"), 0),
                toDouble(templateConfig.get("wavelength"), 0),
                toDouble(templateConfig.get("amplitude"), 0),
                toDoubleArray(templateConfig.get("scalingFactors"), 1));
    }

    public TemplateSeasonalGenerator(double mean, double sigma, double trend, double wavelength, double amplitude,
                                     double[] scalingFactors) {
        this.trend = trend;
        this.wavelength = wavelength;
        this.amplitude = amplitude;
        this.scalingFactors = scalingFactors;

        this.generator = new NormalDistribution(mean, sigma);
    }

    @Override
    public void init() {
        // left blank
    }

    @Override
    public Object next() {
        step++;
        return (long) Math.max((generator.sample()
                + (trend * step)
                + (wavelength == 0d ? 0 : Math.sin(step / wavelength * 2 * Math.PI) * amplitude))
                * makeScalingFactor(step), 0);
    }

    private double makeScalingFactor(long step) {
        double offset = step / wavelength - 0.5 + scalingFactors.length;
        int i = (int) Math.floor(offset) % scalingFactors.length;
        int j = (int) Math.ceil(offset) % scalingFactors.length;

        double shift = offset - Math.floor(offset);

        return (1 - shift) * scalingFactors[i] + shift * scalingFactors[j];
    }

    private static double toDouble(Object obj, double defaultValue) {
        if (obj == null) {
            return defaultValue;
        }
        return Double.valueOf(obj.toString());
    }

    private static double[] toDoubleArray(Object obj, double defaultValue) {
        if (obj == null) {
            double[] values = new double[1];
            Arrays.fill(values, defaultValue);
            return values;
        }

        List<Double> userValues = (List<Double>) obj;
        double[] values = new double[userValues.size()];
        for (int i = 0; i < userValues.size(); i++) {
            values[i] = userValues.get(i);
        }
        return values;
    }

    public static void main(String[] args) {
        TemplateSeasonalGenerator gen = new TemplateSeasonalGenerator(80, 5, 0.02, 24, 40, new double[] { 0.5, 0.9, 1, 1, 1, 0.8, 0.6 });
        for (int i = 0; i < 336; i++) {
            System.out.println(gen.next());
        }
    }
}
