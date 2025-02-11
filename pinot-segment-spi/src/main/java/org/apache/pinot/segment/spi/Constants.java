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
package org.apache.pinot.segment.spi;

public class Constants {
  private Constants() {
  }

  public static final int EOF = Integer.MIN_VALUE;
  public static final int UNKNOWN_CARDINALITY = Integer.MIN_VALUE;

  public static final String HLL_LOG2M_KEY = "log2m";
  public static final String HLLPLUS_ULL_P_KEY = "p";
  public static final String HLLPLUS_SP_KEY = "sp";
  public static final String CPCSKETCH_LGK_KEY = "lgK";
  public static final String THETA_TUPLE_SKETCH_NOMINAL_ENTRIES = "nominalEntries";
  public static final String THETA_TUPLE_SKETCH_SAMPLING_PROBABILITY = "samplingProbability";
  public static final String PERCENTILETDIGEST_COMPRESSION_FACTOR_KEY = "compressionFactor";
  public static final String SUMPRECISION_PRECISION_KEY = "precision";
  public static final String KLL_DOUBLE_SKETCH_K = "K";
}
