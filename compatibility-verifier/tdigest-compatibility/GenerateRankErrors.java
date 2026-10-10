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

import com.tdunning.math.stats.Centroid;
import com.tdunning.math.stats.TDigest;
import java.util.Arrays;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.SplittableRandom;
import org.apache.pinot.segment.local.utils.TDigestUtils;


/// Independent legacy rank oracle, run sequentially with the pinned baseline utility and t-digest 3.3 only.
public final class GenerateRankErrors {
  private static final double[] QUANTILES = {0, 0.5, 0.75, 0.95, 0.99, 1};
  private static final String[] DISTRIBUTIONS = {"UNIFORM", "SKEWED", "HEAVY_TAIL", "BIMODAL", "DUPLICATE_HEAVY"};
  private static final String[] ORDERS = {"ORIGINAL", "REVERSED", "RANDOMIZED"};
  private static final String[] INPUTS = {"SERVER_LOCAL", "DISTRIBUTED"};

  private GenerateRankErrors() {
  }

  public static void main(String[] args)
      throws Exception {
    System.out.println("# mergeOrders=" + String.join(",", ORDERS));
    System.out.println("# reducerInputs=" + String.join(",", INPUTS));
    for (int fanIn : new int[]{8, 32, 128}) {
      for (int compression : new int[]{20, 100, 1000}) {
        for (String distribution : DISTRIBUTIONS) {
          for (String order : ORDERS) {
            for (String input : INPUTS) {
              Reference reference = referenceCase(fanIn, compression, distribution, order, input);
              System.out.print(fanIn + "/" + compression + "/" + distribution + "/" + order + "/" + input
                  + "," + reference.inputSha256());
              for (double error : reference.rankErrors()) {
                System.out.print("," + error);
              }
              System.out.println();
            }
          }
        }
      }
    }
  }

  private record Reference(String inputSha256, double[] rankErrors) {
  }

  private static Reference referenceCase(int fanIn, int compression, String distribution, String order, String input)
      throws Exception {
    TDigest[] sources = new TDigest[fanIn];
    double[] raw = new double[fanIn * 64];
    for (int i = 0; i < fanIn; i++) {
      TDigest digest = TDigestUtils.createMergingDigest(compression);
      SplittableRandom random = new SplittableRandom(0x5EEDL + i);
      for (int j = 0; j < 64; j++) {
        double value = value(distribution, random.nextDouble());
        raw[64 * i + j] = value;
        digest.add(value);
      }
      sources[i] = input.equals("DISTRIBUTED") ? TDigestUtils.deserialize(TDigestUtils.serialize(digest)) : digest;
    }
    TDigest result = TDigestUtils.createMergingDigest(compression);
    int[] sourceIndexes = indexes(fanIn, order);
    for (int index : sourceIndexes) {
      if (result.size() == 0) {
        result = sources[index];
      } else {
        result.add(sources[index]);
      }
    }
    // Match the final public-compression phase in the historical test before measuring quantiles.
    for (Centroid centroid : result.centroids()) {
      if (!Double.isFinite(centroid.mean()) || centroid.count() <= 0) {
        throw new IllegalStateException("Invalid legacy reference centroid");
      }
    }
    Arrays.sort(raw);
    double[] errors = new double[QUANTILES.length];
    for (int i = 0; i < errors.length; i++) {
      errors[i] = rankError(raw, result.quantile(QUANTILES[i]), QUANTILES[i]);
    }
    ByteBuffer encodedRaw = ByteBuffer.allocate(Double.BYTES * raw.length + Integer.BYTES * sourceIndexes.length);
    for (double value : raw) {
      encodedRaw.putDouble(value);
    }
    for (int sourceIndex : sourceIndexes) {
      encodedRaw.putInt(sourceIndex);
    }
    String inputSha256 = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(encodedRaw.array()));
    return new Reference(inputSha256, errors);
  }

  private static double value(String distribution, double unitValue) {
    return switch (distribution) {
      case "UNIFORM" -> unitValue;
      case "SKEWED" -> StrictMath.pow(unitValue, 8);
      case "HEAVY_TAIL" -> StrictMath.pow(1 - 0.999 * unitValue, -1.5) - 1;
      case "BIMODAL" -> unitValue < 0.5 ? 0.1 + 0.2 * unitValue : 0.7 + 0.2 * unitValue;
      case "DUPLICATE_HEAVY" -> unitValue < 0.7 ? 0.1 : unitValue < 0.9 ? 0.5 : 0.9;
      default -> throw new IllegalArgumentException("Unknown distribution: " + distribution);
    };
  }

  private static int[] indexes(int fanIn, String order) {
    int[] indexes = new int[fanIn];
    for (int i = 0; i < fanIn; i++) {
      indexes[i] = i;
    }
    if (order.equals("REVERSED")) {
      for (int i = 0; i < fanIn / 2; i++) {
        int previous = indexes[i];
        indexes[i] = indexes[fanIn - i - 1];
        indexes[fanIn - i - 1] = previous;
      }
    } else if (order.equals("RANDOMIZED")) {
      SplittableRandom random = new SplittableRandom(0xBADC0FFEE0DDF00DL + fanIn);
      for (int i = fanIn - 1; i > 0; i--) {
        int index = random.nextInt(i + 1);
        int previous = indexes[i];
        indexes[i] = indexes[index];
        indexes[index] = previous;
      }
    }
    return indexes;
  }

  private static double rankError(double[] raw, double value, double quantile) {
    for (double observed : raw) {
      if (Math.abs(observed - value) <= 1e-12 * Math.max(1, Math.max(Math.abs(observed), Math.abs(value)))) {
        value = observed;
        break;
      }
    }
    int lower = 0;
    int upper = 0;
    while (lower < raw.length && raw[lower] < value) {
      lower++;
    }
    while (upper < raw.length && raw[upper] <= value) {
      upper++;
    }
    return Math.max(0, Math.max((double) lower / raw.length - quantile, quantile - (double) upper / raw.length));
  }
}
