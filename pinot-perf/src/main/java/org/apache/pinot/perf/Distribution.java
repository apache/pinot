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
package org.apache.pinot.perf;

import java.util.Arrays;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;
import org.apache.commons.lang3.tuple.Pair;


public enum Distribution {
  NORMAL {
    @Override
    public DoubleSupplier createDouble(long seed, double... params) {
      Random random = new Random(seed);
      return () -> random.nextGaussian() * params[1] + params[0];
    }
  },
  UNIFORM {
    @Override
    public DoubleSupplier createDouble(long seed, double... params) {
      Random random = new Random(seed);
      return () -> (params[1] - params[0]) * random.nextDouble() + params[0];
    }
  },
  EXP {
    @Override
    public DoubleSupplier createDouble(long seed, double... params) {
      Random random = new Random(seed);
      return () -> -(Math.log(random.nextDouble()) / params[0]);
    }
  },
  POWER {
    @Override
    public DoubleSupplier createDouble(long seed, double... params) {
      long min = (long) params[0];
      long max = (long) params[1];
      double alpha = params[2];
      SplittableRandom random = new SplittableRandom(seed);
      return () -> (Math.pow((Math.pow(max, alpha + 1)
          - Math.pow(min, alpha + 1) * (random.nextDouble() + 1)), 1D / (alpha + 1)));
    }
  };

  public LongSupplier createLong(long seed, double... params) {
    DoubleSupplier source = createDouble(seed, params);
    return () -> (long) source.getAsDouble();
  }

  public abstract DoubleSupplier createDouble(long seed, double... params);

  public static LongSupplier createLongSupplier(long seed, String spec) {
    Pair<Distribution, double[]> parsed = parse(spec);
    return parsed.getKey().createLong(seed, parsed.getValue());
  }

  public static DoubleSupplier createDoubleSupplier(long seed, String spec) {
    Pair<Distribution, double[]> parsed = parse(spec);
    return parsed.getKey().createDouble(seed, parsed.getValue());
  }

  private static Pair<Distribution, double[]> parse(String spec) {
    int paramsStart = spec.indexOf('(');
    int paramsEnd = spec.indexOf(')');
    double[] params = Arrays.stream(spec.substring(paramsStart + 1, paramsEnd).split(","))
        .mapToDouble(s -> Double.parseDouble(s.trim()))
        .toArray();
    String dist = spec.substring(0, paramsStart).toUpperCase();
    return Pair.of(Distribution.valueOf(dist), params);
  }
}
