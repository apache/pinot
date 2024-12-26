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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Random;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;
import org.apache.commons.lang3.tuple.Pair;


public enum Distribution {
  NORMAL {
    @Override
    public DataSupplier createSupplier(long seed, double... params) {
      DoubleGenerator generator = (r) -> r.nextGaussian() * params[1] + params[0];
      return new DataSupplier(seed, generator);
    }
  },
  UNIFORM {
    @Override
    public DataSupplier createSupplier(long seed, double... params) {
      DoubleGenerator generator = (r) -> (params[1] - params[0]) * r.nextDouble() + params[0];
      return new DataSupplier(seed, generator);
    }
  },
  EXP {
    @Override
    public DataSupplier createSupplier(long seed, double... params) {
      DoubleGenerator generator = (r) -> -(Math.log(r.nextDouble()) / params[0]);
      return new DataSupplier(seed, generator);
    }
  },
  POWER {
    @Override
    public DataSupplier createSupplier(long seed, double... params) {
      long min = (long) params[0];
      long max = (long) params[1];
      double alpha = params[2];

      DoubleGenerator generator = (r) -> (Math.pow((Math.pow(max, alpha + 1)
          - Math.pow(min, alpha + 1) * (r.nextDouble() + 1)), 1D / (alpha + 1)));

      return new DataSupplier(seed, generator);
    }
  };

  public interface DoubleGenerator {
    double nextDouble(Random r);
  }

  public static class DataSupplier implements Cloneable, DoubleSupplier, LongSupplier {
    private Random _random;
    private DoubleGenerator _generator;
    private long _initialSeed;
    private boolean _initialSeedKnown;
    private Random _initialRandom;

    public DataSupplier(long initialSeed, DoubleGenerator generator) {
      _initialSeed = initialSeed;
      _initialSeedKnown = true;
      _random = new Random(_initialSeed);
      _generator = generator;
    }

    public DataSupplier(Random random, DoubleGenerator generator) {
      _initialSeed = -1;
      _initialSeedKnown = false;
      _initialRandom = random;
      _random = clone(random);
      _generator = generator;
    }

    public long getAsLong() {
      return (long) _generator.nextDouble(_random);
    }

    public double getAsDouble() {
      return _generator.nextDouble(_random);
    }

    public void reset() {
      if (_initialSeedKnown) {
        _random = new Random(_initialSeed);
      } else {
        _random = clone(_initialRandom);
      }
    }

    public void snapshot() {
      _initialSeedKnown = false;
      _initialSeed = -1;
      _initialRandom = clone(_random);
    }

    @Override
    protected Object clone()
    throws CloneNotSupportedException {
      return new DataSupplier(clone(_random), _generator);
    }

    private static Random clone(Random random) {
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(random);
        oos.close();
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
        return (Random) (ois.readObject());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public abstract DataSupplier createSupplier(long seed, double... params);

  public static DataSupplier createSupplier(long seed, String spec) {
    Pair<Distribution, double[]> parsed = parse(spec);
    return parsed.getKey().createSupplier(seed, parsed.getValue());
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
