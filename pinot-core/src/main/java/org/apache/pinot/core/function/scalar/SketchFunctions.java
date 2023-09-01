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
package org.apache.pinot.core.function.scalar;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import java.math.BigDecimal;
import java.util.Base64;
import javax.annotation.Nullable;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.AnotB;
import org.apache.datasketches.theta.Intersection;
import org.apache.datasketches.theta.SetOperationBuilder;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.tuple.aninteger.IntegerSketch;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Inbuilt Sketch Transformation Functions
 * The functions can be used as UDFs in Query when added in the FunctionRegistry.
 * @ScalarFunction annotation is used with each method for the registration
 *
 * Note these will just make sketches that contain a single item, these are intended to be used during ingestion to
 * create sketches from raw data, which can be rolled up later.
 *
 * Note this is defined in pinot-core rather than pinot-common because pinot-core has dependencies on
 * datasketches/clearspring analytics.
 *
 * Example usage:
 *
 * {
 *   "transformConfigs": [
 *     {
 *       "columnName": "players",
 *       "transformFunction": "toThetaSketch(playerID)"
 *     },
 *     {
 *       "columnName": "players",
 *       "transformFunction": "toThetaSketch(playerID, 1024)"
 *     },
 *     {
 *       "columnName": "names",
 *       "transformFunction": "toHLL(playerName)"
 *     },
 *     {
 *       "columnName": "names",
 *       "transformFunction": "toHLL(playerName, 8)"
 *     }
 *   ]
 * }
 */
public class SketchFunctions {
  private static final SetOperationBuilder SET_OPERATION_BUILDER = new SetOperationBuilder();

  private SketchFunctions() {
  }

  /**
   * Create a Theta Sketch containing the input
   *
   * @param input an Object we want to insert into the sketch, may be null to return an empty sketch
   * @return serialized theta sketch as bytes
   */
  @ScalarFunction(nullableParameters = true)
  public static byte[] toThetaSketch(@Nullable Object input) {
    return toThetaSketch(input, CommonConstants.Helix.DEFAULT_THETA_SKETCH_NOMINAL_ENTRIES);
  }

  /**
   * Create a Theta Sketch containing the input, with a configured nominal entries
   *
   * @param input an Object we want to insert into the sketch, may be null to return an empty sketch
   * @param nominalEntries number of nominal entries the sketch is configured to keep
   * @return serialized theta sketch as bytes
   */
  @ScalarFunction(nullableParameters = true)
  public static byte[] toThetaSketch(@Nullable Object input, int nominalEntries) {
    UpdateSketch sketch = Sketches.updateSketchBuilder().setNominalEntries(nominalEntries).build();
    if (input != null) {
      if (input instanceof Integer) {
        sketch.update((Integer) input);
      } else if (input instanceof Long) {
        sketch.update((Long) input);
      } else if (input instanceof Float) {
        sketch.update((Float) input);
      } else if (input instanceof Double) {
        sketch.update((Double) input);
      } else if (input instanceof BigDecimal) {
        sketch.update(((BigDecimal) input).toString());
      } else if (input instanceof String) {
        sketch.update((String) input);
      } else if (input instanceof byte[]) {
        sketch.update((byte[]) input);
      } else {
        throw new IllegalArgumentException(
            "Unrecognised input type for Theta sketch: " + input.getClass().getSimpleName());
      }
    }
    return ObjectSerDeUtils.DATA_SKETCH_SER_DE.serialize(sketch.compact());
  }

  /**
   * Create a HyperLogLog containing the input
   *
   * @param input an Object we want to insert into the HLL, may be null to return an empty HLL
   * @return serialized HLL as bytes
   */
  @ScalarFunction(nullableParameters = true)
  public static byte[] toHLL(@Nullable Object input) {
    return toHLL(input, CommonConstants.Helix.DEFAULT_HYPERLOGLOG_LOG2M);
  }

  /**
   * Create a HyperLogLog containing the input, with a configurable log2m
   *
   * @param input an Object we want to insert into the HLL, may be null to return an empty HLL
   * @param log2m the log2m value for the created HyperLogLog
   * @return serialized HLL as bytes
   */
  @ScalarFunction(nullableParameters = true)
  public static byte[] toHLL(@Nullable Object input, int log2m) {
    HyperLogLog hll = new HyperLogLog(log2m);
    if (input != null) {
      hll.offer(input);
    }
    return ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.serialize(hll);
  }

  /**
   * Create a Tuple Sketch containing the key and value supplied
   *
   * @param key an Object we want to insert as the key of the sketch, may be null to return an empty sketch
   * @param value an Integer we want to associate as the value to go along with the key, may be null to return an
   *              empty sketch
   * @return serialized tuple sketch as bytes
   */
  @ScalarFunction(nullableParameters = true)
  public static byte[] toIntegerSumTupleSketch(@Nullable Object key, @Nullable Integer value) {
    return toIntegerSumTupleSketch(key, value, CommonConstants.Helix.DEFAULT_TUPLE_SKETCH_LGK);
  }

  /**
   * Create a Tuple Sketch containing the key and value supplied
   *
   * @param key an Object we want to insert as the key of the sketch, may be null to return an empty sketch
   * @param value an Integer we want to associate as the value to go along with the key, may be null to return an
   *              empty sketch
   * @param lgK integer representing the log of the maximum number of retained entries in the sketch, between 4 and 26
   * @return serialized tuple sketch as bytes
   */
  @ScalarFunction(nullableParameters = true)
  public static byte[] toIntegerSumTupleSketch(@Nullable Object key, Integer value, int lgK) {
    IntegerSketch is = new IntegerSketch(lgK, IntegerSummary.Mode.Sum);
    if (value != null && key != null) {
      if (key instanceof Integer) {
        is.update((Integer) key, value);
      } else if (key instanceof Long) {
        is.update((Long) key, value);
      } else if (key instanceof Float) {
        is.update((float) key, value);
      } else if (key instanceof Double) {
        is.update((double) key, value);
      } else if (key instanceof BigDecimal) {
        is.update(((BigDecimal) key).toString(), value);
      } else if (key instanceof String) {
        is.update((String) key, value);
      } else if (key instanceof byte[]) {
        is.update((byte[]) key, value);
      } else {
        throw new IllegalArgumentException("Unrecognised key type for Theta sketch: " + key.getClass().getSimpleName());
      }
    }
    return ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.serialize(is.compact());
  }

  @ScalarFunction(names = {"getThetaSketchEstimate", "get_theta_sketch_estimate"})
  public static long getThetaSketchEstimate(Object sketchObject) {
    return Math.round(asThetaSketch(sketchObject).getEstimate());
  }

  @ScalarFunction(names = {"thetaSketchUnion", "theta_sketch_union"})
  public static Sketch thetaSketchUnion(Object o1, Object o2) {
    return thetaSketchUnionVar(o1, o2);
  }

  @ScalarFunction(names = {"thetaSketchUnion", "theta_sketch_union"})
  public static Sketch thetaSketchUnion(Object o1, Object o2, Object o3) {
    return thetaSketchUnionVar(o1, o2, o3);
  }

  @ScalarFunction(names = {"thetaSketchUnion", "theta_sketch_union"})
  public static Sketch thetaSketchUnion(Object o1, Object o2, Object o3, Object o4) {
    return thetaSketchUnionVar(o1, o2, o3, o4);
  }

  @ScalarFunction(names = {"thetaSketchUnion", "theta_sketch_union"})
  public static Sketch thetaSketchUnion(Object o1, Object o2, Object o3, Object o4, Object o5) {
    return thetaSketchUnionVar(o1, o2, o3, o4, o5);
  }

  @ScalarFunction(names = {"thetaSketchIntersect", "theta_sketch_intersect"})
  public static Sketch thetaSketchIntersect(Object o1, Object o2) {
    return thetaSketchIntersectVar(o1, o2);
  }

  @ScalarFunction(names = {"thetaSketchIntersect", "theta_sketch_intersect"})
  public static Sketch thetaSketchIntersect(Object o1, Object o2, Object o3) {
    return thetaSketchIntersectVar(o1, o2, o3);
  }

  @ScalarFunction(names = {"thetaSketchIntersect", "theta_sketch_intersect"})
  public static Sketch thetaSketchIntersect(Object o1, Object o2, Object o3, Object o4) {
    return thetaSketchIntersectVar(o1, o2, o3, o4);
  }

  @ScalarFunction(names = {"thetaSketchIntersect", "theta_sketch_intersect"})
  public static Sketch thetaSketchIntersect(Object o1, Object o2, Object o3, Object o4, Object o5) {
    return thetaSketchIntersectVar(o1, o2, o3, o4, o5);
  }

  @ScalarFunction(names = {"thetaSketchDiff", "theta_sketch_diff"})
  public static Sketch thetaSketchDiff(Object sketchObjectA, Object sketchObjectB) {
    AnotB diff = SET_OPERATION_BUILDER.buildANotB();
    diff.setA(asThetaSketch(sketchObjectA));
    diff.notB(asThetaSketch(sketchObjectB));
    return diff.getResult(false, null, false);
  }

  private static Sketch thetaSketchUnionVar(Object... sketchObjects) {
    Union union = SET_OPERATION_BUILDER.buildUnion();
    for (Object sketchObj : sketchObjects) {
      union.union(asThetaSketch(sketchObj));
    }
    return union.getResult(false, null);
  }

  private static Sketch thetaSketchIntersectVar(Object... sketchObjects) {
    Intersection intersection = SET_OPERATION_BUILDER.buildIntersection();
    for (Object sketchObj : sketchObjects) {
      intersection.intersect(asThetaSketch(sketchObj));
    }
    return intersection.getResult(false, null);
  }

  private static Sketch asThetaSketch(Object sketchObj) {
    if (sketchObj instanceof String) {
      byte[] decoded = Base64.getDecoder().decode((String) sketchObj);
      return Sketches.wrapSketch(Memory.wrap((decoded)));
    } else if (sketchObj instanceof Sketch) {
      return (Sketch) sketchObj;
    } else if (sketchObj instanceof byte[]) {
      return Sketches.wrapSketch(Memory.wrap((byte[]) sketchObj));
    } else {
      throw new RuntimeException("Exception occurred getting estimate from Theta Sketch, unsupported Object type: "
          + sketchObj.getClass());
    }
  }
}
