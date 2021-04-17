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
package org.apache.pinot.core.segment.processing.framework;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.core.segment.processing.collector.GenericRowSorter;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link GenericRowSorter}
 */
public class GenericRowSorterTest {

  private static final Random RANDOM = new Random(10);

  @Test
  public void testSort() {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("stringCol", FieldSpec.DataType.STRING)
        .addSingleValueDimension("intCol", FieldSpec.DataType.INT).addMetric("longCol", FieldSpec.DataType.LONG)
        .addMetric("doubleCol", FieldSpec.DataType.DOUBLE).build();

    List<GenericRow> rows = new ArrayList<>(1000);
    List<Object[]> ogRows = new ArrayList<>(1000);
    for (int i = 0; i < 1000; i++) {
      Object[] ogRow = new Object[]{RandomStringUtils.randomAlphabetic(5), RANDOM.nextInt(10), RANDOM.nextLong(), RANDOM
          .nextDouble()};
      GenericRow row = new GenericRow();
      row.putValue("stringCol", ogRow[0]);
      row.putValue("intCol", ogRow[1]);
      row.putValue("longCol", ogRow[2]);
      row.putValue("doubleCol", ogRow[3]);
      rows.add(row);
      ogRows.add(ogRow);
    }

    GenericRowSorter sorter = new GenericRowSorter(Lists.newArrayList("intCol", "stringCol", "doubleCol"), schema);
    sorter.sort(rows);

    ogRows.sort(Comparator.comparingInt((Object[] o) -> (int) o[1]).thenComparing(o -> (String) o[0])
        .thenComparingDouble(o -> (double) o[3]));

    for (int i = 0; i < 1000; i++) {
      GenericRow r = rows.get(i);
      Object[] ogRow = ogRows.get(i);
      Assert.assertEquals(ogRow[0], r.getValue("stringCol"));
      Assert.assertEquals(ogRow[1], r.getValue("intCol"));
      Assert.assertEquals(ogRow[2], r.getValue("longCol"));
      Assert.assertEquals(ogRow[3], r.getValue("doubleCol"));
    }
  }
}
