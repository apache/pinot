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
package org.apache.pinot.spi.data.readers;

import org.testng.Assert;
import org.testng.annotations.Test;

public class VectorTest {

  @Test
  public void testFloatVector() {
    int dimension = 2;
    float[] values = new float[] {1.2f, 3.4f};
    Vector floatVector = new Vector(dimension, values);

    Assert.assertEquals(floatVector.getDimension(), dimension);
    Assert.assertEquals(floatVector.getFloatValues(), values);
    Assert.assertEquals(floatVector.getType(), Vector.VectorType.FLOAT);

    byte[] bytes = floatVector.toBytes();
    Vector deserializedVector = Vector.fromBytes(bytes);

    Assert.assertEquals(deserializedVector.getDimension(), dimension);
    Assert.assertEquals(deserializedVector.getFloatValues(), values);
    Assert.assertEquals(deserializedVector.getType(), Vector.VectorType.FLOAT);
  }

  @Test
  public void testIntVector() {
    int dimension = 2;
    int[] values = new int[] {1, 2};
    Vector intVector = new Vector(dimension, values);

    Assert.assertEquals(intVector.getDimension(), dimension);
    Assert.assertEquals(intVector.getIntValues(), values);
    Assert.assertEquals(intVector.getType(), Vector.VectorType.INT);

    byte[] bytes = intVector.toBytes();
    Vector deserializedVector = Vector.fromBytes(bytes);

    Assert.assertEquals(deserializedVector.getDimension(), dimension);
    Assert.assertEquals(deserializedVector.getIntValues(), values);
    Assert.assertEquals(deserializedVector.getType(), Vector.VectorType.INT);
  }

  @Test
  public void testFloatVectorToString() {
    int dimension = 2;
    float[] values = new float[] {1.2f, 3.4f};
    Vector vector = new Vector(dimension, values);

    String expected = "FLOAT,2,1.2,3.4";
    Assert.assertEquals(vector.toString(), expected);

    Vector fromStringVector = Vector.fromString(expected);
    Assert.assertEquals(fromStringVector.getDimension(), dimension);
    Assert.assertEquals(fromStringVector.getFloatValues(), values);
    Assert.assertEquals(fromStringVector.getType(), Vector.VectorType.FLOAT);
  }

  @Test
  public void testIntVectorToString() {
    int dimension = 3;
    int[] values = new int[] {5, 6, 7};
    Vector vector = new Vector(dimension, values);

    String expected = "INT,3,5,6,7";
    Assert.assertEquals(vector.toString(), expected);

    Vector fromStringVector = Vector.fromString(expected);
    Assert.assertEquals(fromStringVector.getDimension(), dimension);
    Assert.assertEquals(fromStringVector.getIntValues(), values);
    Assert.assertEquals(fromStringVector.getType(), Vector.VectorType.INT);
  }
}
