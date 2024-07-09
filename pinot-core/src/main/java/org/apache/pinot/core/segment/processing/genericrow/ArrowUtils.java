package org.apache.pinot.core.segment.processing.genericrow;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;


public class ArrowUtils {
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

    for (int i = 0; i < length; i++) {
      //FIXME: Find arrow way to recreate list vector
      //vector.accept()
    }
  }
}
