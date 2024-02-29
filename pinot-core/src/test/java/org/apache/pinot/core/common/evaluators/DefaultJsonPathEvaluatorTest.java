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
package org.apache.pinot.core.common.evaluators;

import java.nio.charset.StandardCharsets;
import org.apache.pinot.segment.spi.evaluator.json.JsonPathEvaluator;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;


public class DefaultJsonPathEvaluatorTest {
  @Test
  public void testNonDictIntegerArray() {
    String json = "{\"values\": [1, 2, 3, 4, 5]}";
    String path = "$.values[0:3]";
    JsonPathEvaluator evaluator = DefaultJsonPathEvaluator.create(path, new int[]{});
    ForwardIndexReader<ForwardIndexReaderContext> reader = mock(ForwardIndexReader.class);
    when(reader.isDictionaryEncoded()).thenReturn(false);
    when(reader.getBytes(eq(0), any())).thenReturn(json.getBytes(StandardCharsets.UTF_8));
    when(reader.getStoredType()).thenReturn(FieldSpec.DataType.STRING);
    when(reader.createContext()).thenReturn(null);

    // Read as ints
    int[][] buffer = new int[1][3];
    evaluator.evaluateBlock(new int[]{0}, 1, reader, null, null, null, buffer);
    assertArrayEquals(buffer, new int[][]{{1, 2, 3}});

    // Read as longs
    long[][] longBuffer = new long[1][3];
    evaluator.evaluateBlock(new int[]{0}, 1, reader, null, null, null, longBuffer);
    assertArrayEquals(longBuffer, new long[][]{{1, 2, 3}});

    // Read as floats
    float[][] floatBuffer = new float[1][3];
    evaluator.evaluateBlock(new int[]{0}, 1, reader, null, null, null, floatBuffer);
    assertArrayEquals(floatBuffer, new float[][]{{1.0f, 2.0f, 3.0f}});

    // Read as doubles
    double[][] doubleBuffer = new double[1][3];
    evaluator.evaluateBlock(new int[]{0}, 1, reader, null, null, null, doubleBuffer);
    assertArrayEquals(doubleBuffer, new double[][]{{1.0, 2.0, 3.0}});

    // Read as strings
    String[][] stringBuffer = new String[1][3];
    evaluator.evaluateBlock(new int[]{0}, 1, reader, null, null, null, stringBuffer);
    assertArrayEquals(stringBuffer, new String[][]{{"1", "2", "3"}});
  }

  @Test
  public void testNonDictStringArray() {
    String json = "{\"values\": [\"1\", \"2\", \"3\", \"4\", \"5\"]}";
    String path = "$.values[0:3]";
    JsonPathEvaluator evaluator = DefaultJsonPathEvaluator.create(path, new int[]{});
    ForwardIndexReader<ForwardIndexReaderContext> reader = mock(ForwardIndexReader.class);
    when(reader.isDictionaryEncoded()).thenReturn(false);
    when(reader.getBytes(eq(0), any())).thenReturn(json.getBytes(StandardCharsets.UTF_8));
    when(reader.getStoredType()).thenReturn(FieldSpec.DataType.STRING);
    when(reader.createContext()).thenReturn(null);

    // Read as ints
    int[][] buffer = new int[1][3];
    evaluator.evaluateBlock(new int[]{0}, 1, reader, null, null, null, buffer);
    assertArrayEquals(buffer, new int[][]{{1, 2, 3}});

    // Read as longs
    long[][] longBuffer = new long[1][3];
    evaluator.evaluateBlock(new int[]{0}, 1, reader, null, null, null, longBuffer);
    assertArrayEquals(longBuffer, new long[][]{{1, 2, 3}});

    // Read as floats
    float[][] floatBuffer = new float[1][3];
    evaluator.evaluateBlock(new int[]{0}, 1, reader, null, null, null, floatBuffer);
    assertArrayEquals(floatBuffer, new float[][]{{1.0f, 2.0f, 3.0f}});

    // Read as doubles
    double[][] doubleBuffer = new double[1][3];
    evaluator.evaluateBlock(new int[]{0}, 1, reader, null, null, null, doubleBuffer);
    assertArrayEquals(doubleBuffer, new double[][]{{1.0, 2.0, 3.0}});

    // Read as strings
    String[][] stringBuffer = new String[1][3];
    evaluator.evaluateBlock(new int[]{0}, 1, reader, null, null, null, stringBuffer);
    assertArrayEquals(stringBuffer, new String[][]{{"1", "2", "3"}});
  }

  @Test
  public void testNonDictDoubleArray() {
    String json = "{\"values\": [1.0, 2.0, 3.0, 4.0, 5.0]}";
    String path = "$.values[0:3]";
    JsonPathEvaluator evaluator = DefaultJsonPathEvaluator.create(path, new int[]{});
    ForwardIndexReader<ForwardIndexReaderContext> reader = mock(ForwardIndexReader.class);
    when(reader.isDictionaryEncoded()).thenReturn(false);
    when(reader.getBytes(eq(0), any())).thenReturn(json.getBytes(StandardCharsets.UTF_8));
    when(reader.getStoredType()).thenReturn(FieldSpec.DataType.STRING);
    when(reader.createContext()).thenReturn(null);

    // Read as ints
    int[][] buffer = new int[1][3];
    evaluator.evaluateBlock(new int[]{0}, 1, reader, null, null, null, buffer);
    assertArrayEquals(buffer, new int[][]{{1, 2, 3}});

    // Read as longs
    long[][] longBuffer = new long[1][3];
    evaluator.evaluateBlock(new int[]{0}, 1, reader, null, null, null, longBuffer);
    assertArrayEquals(longBuffer, new long[][]{{1, 2, 3}});

    // Read as floats
    float[][] floatBuffer = new float[1][3];
    evaluator.evaluateBlock(new int[]{0}, 1, reader, null, null, null, floatBuffer);
    assertArrayEquals(floatBuffer, new float[][]{{1.0f, 2.0f, 3.0f}});

    // Read as doubles
    double[][] doubleBuffer = new double[1][3];
    evaluator.evaluateBlock(new int[]{0}, 1, reader, null, null, null, doubleBuffer);
    assertArrayEquals(doubleBuffer, new double[][]{{1.0, 2.0, 3.0}});

    // Read as strings
    String[][] stringBuffer = new String[1][3];
    evaluator.evaluateBlock(new int[]{0}, 1, reader, null, null, null, stringBuffer);
    assertArrayEquals(stringBuffer, new String[][]{{"1.0", "2.0", "3.0"}});
  }
}
