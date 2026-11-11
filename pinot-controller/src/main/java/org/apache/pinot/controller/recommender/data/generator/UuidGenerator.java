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

import com.google.common.annotations.VisibleForTesting;
import java.util.Random;
import java.util.UUID;


/// Generates canonical, parseable UUID-format strings for a column of type UUID. UUID is a logical type whose
/// recommender Avro schema is a `string` annotated with `logicalType: uuid` (see
/// `AvroSchemaUtil#toAvroSchemaJsonObject`), so the generated value is the canonical string form and the writer
/// serializes it directly.
///
/// The number of distinct values is bounded by the requested cardinality: a fixed random high-order half is combined
/// with a low-order counter that cycles `[1, cardinality]`, mirroring `StringGenerator`. These are deliberately not
/// RFC-4122 version-4 UUIDs (the version/variant bits are not set); sample data only needs distinct,
/// canonically-formatted, parseable values.
public class UuidGenerator implements Generator {
  private static final double DEFAULT_NUMBER_OF_VALUES_PER_ENTRY = 1;

  private final int _cardinality;
  private final double _numberOfValuesPerEntry;
  private final Random _random;
  private final long _mostSignificantBits;
  private long _counter;

  public UuidGenerator(Integer cardinality, Double numberOfValuesPerEntry) {
    this(cardinality, numberOfValuesPerEntry, new Random(System.currentTimeMillis()));
  }

  @VisibleForTesting
  UuidGenerator(Integer cardinality, Double numberOfValuesPerEntry, Random random) {
    _cardinality = cardinality;
    _numberOfValuesPerEntry =
        numberOfValuesPerEntry != null ? numberOfValuesPerEntry : DEFAULT_NUMBER_OF_VALUES_PER_ENTRY;
    _random = random;
    _mostSignificantBits = random.nextLong();
  }

  @Override
  public void init() {
  }

  @Override
  public Object next() {
    if (_numberOfValuesPerEntry == 1) {
      return nextUuid();
    }
    return MultiValueGeneratorHelper.generateMultiValueEntries(_numberOfValuesPerEntry, _random, this::nextUuid);
  }

  private String nextUuid() {
    if (_counter >= _cardinality) {
      _counter = 0;
    }
    _counter++;
    return new UUID(_mostSignificantBits, _counter).toString();
  }
}
