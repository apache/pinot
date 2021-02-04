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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Sep 12, 2014
 */

public class NumberGenerator implements Generator {
  private static final Logger LOGGER = LoggerFactory.getLogger(NumberGenerator.class);
  private static final double DEFAULT_NUMBER_OF_VALUES_PER_ENTRY = 1;

  private final int cardinality;
  private final DataType columnType;
  private final double numberOfValuesPerEntry;

  private List<Integer> intValues;
  private List<Double> doubleValues;
  private List<Long> longValues;
  private List<Float> floatValues;

  private final Random random;

  public NumberGenerator(Integer cardinality, DataType type, Double numberOfValuesPerEntry) {
    this.cardinality = cardinality;
    this.numberOfValuesPerEntry =
        numberOfValuesPerEntry != null ? numberOfValuesPerEntry : DEFAULT_NUMBER_OF_VALUES_PER_ENTRY;
    Preconditions.checkState(this.numberOfValuesPerEntry >= 1,
        "Number of values per entry (should be >= 1): " + this.numberOfValuesPerEntry);
    columnType = type;
    random = new Random(System.currentTimeMillis());
  }

  @Override
  public void init() {
    final Random rand = new Random(System.currentTimeMillis());

    switch (columnType) {
      case INT:
        intValues = new ArrayList<Integer>();
        final int start = rand.nextInt(cardinality);
        final int end = start + cardinality;
        for (int i = start; i < end; i++) {
          intValues.add(i);
        }
        break;
      case LONG:
        longValues = new ArrayList<Long>();
        final long longStart = rand.nextInt(cardinality);
        final long longEnd = longStart + cardinality;
        for (long i = longStart; i < longEnd; i++) {
          longValues.add(i);
        }
        break;
      case FLOAT:
        floatValues = new ArrayList<Float>();
        final float floatStart = Math.round(rand.nextFloat()* 100.0f) / 100.0f;
        int floatCounter = 1;
        while (true) {
          floatValues.add(floatStart + floatCounter);
          if (floatCounter == cardinality) {
            break;
          }
          floatCounter++;
        }
        break;
      case DOUBLE:
        doubleValues = new ArrayList<Double>();
        final double doubleStart = Math.round(rand.nextDouble() * 100.0) / 100.0;
        int doubleCounter = 1;
        while (true) {
          doubleValues.add(doubleStart + doubleCounter);
          if (doubleCounter == cardinality) {
            break;
          }
          doubleCounter++;
        }
        break;
      default:
        throw new RuntimeException("number generator can only accept a column of type number and this : " + columnType
            + " is not a supported number type");
    }
  }

  @Override
  public Object next() {
    if (numberOfValuesPerEntry == 1) {
      return getNextNumber();
    }
    return MultiValueGeneratorHelper.generateMultiValueEntries(numberOfValuesPerEntry, random, this::getNextNumber);
  }

  private Number getNextNumber() {
    switch (columnType) {
      case INT:
        return intValues.get(random.nextInt(cardinality));
      case LONG:
        return longValues.get(random.nextInt(cardinality));
      case FLOAT:
        return floatValues.get(random.nextInt(cardinality));
      case DOUBLE:
        return doubleValues.get(random.nextInt(cardinality));
      default:
        throw new RuntimeException("number generator can only accept a column of type number and this : " + columnType
            + " is not a supported number type");
    }
  }

  public static void main(String[] args) {
    final NumberGenerator gen = new NumberGenerator(10000000, DataType.LONG, null);
    gen.init();
    for (int i = 0; i < 1000; i++) {
      System.out.println(gen.next());
    }
  }
}
