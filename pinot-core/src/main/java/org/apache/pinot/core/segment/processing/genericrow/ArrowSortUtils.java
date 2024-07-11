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
package org.apache.pinot.core.segment.processing.genericrow;

import java.util.List;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.util.Text;


/**
 * Use to convert Arrow vectors to Arrow vectors with a given sort order.
 * This implementation can be quite heavy on memory due to intermediate copying
 * of the data.
 * TODO: Find a way to eleminate copying data (if possible use inplace sorting in arrow)
 */
public class ArrowSortUtils {
  public static void inPlaceSortAll(VectorSchemaRoot root, int[] sortIndices) {
    for (FieldVector vector : root.getFieldVectors()) {
      if (vector instanceof IntVector) {
        sortIntVector((IntVector) vector, sortIndices);
      } else if (vector instanceof VarCharVector) {
        sortVarCharVector((VarCharVector) vector, sortIndices);
      } else if (vector instanceof Float4Vector) {
        sortFloat4Vector((Float4Vector) vector, sortIndices);
      } else if (vector instanceof Float8Vector) {
        sortFloat8Vector((Float8Vector) vector, sortIndices);
      } else if (vector instanceof BigIntVector) {
        sortBigIntVector((BigIntVector) vector, sortIndices);
      } else if (vector instanceof VarBinaryVector) {
        sortVarBinaryVector((VarBinaryVector) vector, sortIndices);
      } else if (vector instanceof ListVector) {
        sortListVector((ListVector) vector, sortIndices);
      } else {
        throw new UnsupportedOperationException("Unsupported vector type: " + vector.getClass().getSimpleName());
      }
    }
  }

  private static void sortVarBinaryVector(VarBinaryVector vector, int[] sortIndices) {
    int length = sortIndices.length;
    byte[][] tempArray = new byte[length][];

    for (int i = 0; i < length; i++) {
      tempArray[i] = vector.get(sortIndices[i]);
    }

    for (int i = 0; i < length; i++) {
      vector.set(i, tempArray[i]);
    }
  }

  private static void sortIntVector(IntVector vector, int[] sortIndices) {
    int length = sortIndices.length;
    int[] tempArray = new int[length];

    for (int i = 0; i < length; i++) {
      tempArray[i] = vector.get(sortIndices[i]);
    }

    for (int i = 0; i < length; i++) {
      vector.set(i, tempArray[i]);
    }
  }

  private static void sortVarCharVector(VarCharVector vector, int[] sortIndices) {
    int length = sortIndices.length;
    byte[][] tempArray = new byte[length][];

    for (int i = 0; i < length; i++) {
      tempArray[i] = vector.get(sortIndices[i]);
    }

    for (int i = 0; i < length; i++) {
      vector.set(i, tempArray[i]);
    }
  }

  private static void sortFloat4Vector(Float4Vector vector, int[] sortIndices) {
    int length = sortIndices.length;
    float[] tempArray = new float[length];

    for (int i = 0; i < length; i++) {
      tempArray[i] = vector.get(sortIndices[i]);
    }

    for (int i = 0; i < length; i++) {
      vector.set(i, tempArray[i]);
    }
  }

  private static void sortFloat8Vector(Float8Vector vector, int[] sortIndices) {
    int length = sortIndices.length;
    double[] tempArray = new double[length];

    for (int i = 0; i < length; i++) {
      tempArray[i] = vector.get(sortIndices[i]);
    }

    for (int i = 0; i < length; i++) {
      vector.set(i, tempArray[i]);
    }
  }

  private static void sortBigIntVector(BigIntVector vector, int[] sortIndices) {
    int length = sortIndices.length;
    long[] tempArray = new long[length];

    for (int i = 0; i < length; i++) {
      tempArray[i] = vector.get(sortIndices[i]);
    }

    for (int i = 0; i < length; i++) {
      vector.set(i, tempArray[i]);
    }
  }

  private static void sortListVector(ListVector vector, int[] sortIndices) {
    int length = sortIndices.length;
    Object[] tempArray = new Object[length];

    for (int i = 0; i < length; i++) {
      tempArray[i] = vector.getObject(sortIndices[i]);
    }

    vector.clear();

    UnionListWriter writer = vector.getWriter();
    writer.allocate();

    for (int i = 0; i < length; i++) {
      writer.setPosition(i);
      Object value = tempArray[i];

      if (value == null) {
        writer.writeNull();
      } else if (value instanceof List) {
        writer.startList();
        List<?> list = (List<?>) value;
        for (Object item : list) {
          writeItem(writer, item);
        }
        writer.endList();
      } else {
        // Handle scalar values if needed
        writeItem(writer, value);
      }
    }

    writer.setValueCount(length);
  }

  private static void writeItem(UnionListWriter writer, Object item) {
    if (item instanceof Integer) {
      writer.writeInt((Integer) item);
    } else if (item instanceof Long) {
      writer.writeBigInt((Long) item);
    } else if (item instanceof Float) {
      writer.writeFloat4((Float) item);
    } else if (item instanceof Double) {
      writer.writeFloat8((Double) item);
    } else if (item instanceof Text) {
      writer.writeVarChar(((Text) item));
    } else {
      throw new UnsupportedOperationException("Unsupported type: " + item.getClass().getSimpleName());
    }
  }
}
