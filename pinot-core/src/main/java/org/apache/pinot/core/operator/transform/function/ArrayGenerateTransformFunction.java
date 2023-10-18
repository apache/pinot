package org.apache.pinot.core.operator.transform.function;

import com.google.common.base.Preconditions;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.pinot.spi.data.FieldSpec.DataType;

public class ArrayGenerateTransformFunction implements TransformFunction {
  public static final String FUNCTION_NAME = "GENERATE_ARRAY";

  private final DataType _dataType;

  private final int[] _intArrayLiteral;
  private final long[] _longArrayLiteral;
  private final float[] _floatArrayLiteral;
  private final double[] _doubleArrayLiteral;
  public ArrayGenerateTransformFunction (List<ExpressionContext> literalContexts) {
    Preconditions.checkNotNull(literalContexts);
    if (literalContexts.isEmpty()) {
      _dataType = DataType.UNKNOWN;
      _intArrayLiteral = new int[0];
      _longArrayLiteral = new long[0];
      _floatArrayLiteral = new float[0];
      _doubleArrayLiteral = new double[0];
      return;
    }
    for (ExpressionContext literalContext : literalContexts) {
      Preconditions.checkState(literalContext.getType() == ExpressionContext.Type.LITERAL,
              "ArrayLiteralTransformFunction only takes literals as arguments, found: %s", literalContext);
    }
    // Get the type of the first member in the literalContext and generate an array
    _dataType = literalContexts.get(0).getLiteral().getType();

    switch (_dataType) {
      case INT:
        int startInt = literalContexts.get(0).getLiteral().getIntValue();
        int endInt = literalContexts.get(1).getLiteral().getIntValue();
        int incInt = literalContexts.get(2).getLiteral().getIntValue();
        int size = (endInt - startInt) / incInt +1;
        _intArrayLiteral = new int[size];
        for (int i = 0, value = startInt; i < size ; i++, value += incInt) {
          _intArrayLiteral[i]  = value;
        }
        _longArrayLiteral = null;
        _floatArrayLiteral = null;
        _doubleArrayLiteral = null;
        break;
      case LONG:
        long startLong = Long.parseLong(literalContexts.get(0).getLiteral().getStringValue());
        long endLong = Long.parseLong(literalContexts.get(1).getLiteral().getStringValue());
        long incLong = Long.parseLong(literalContexts.get(2).getLiteral().getStringValue());
        size = (int)((endLong - startLong) / incLong +1);
        _longArrayLiteral = new long[size];
        for (int i = 0; i < size; i++, startLong += incLong) {
        _longArrayLiteral[i] = startLong;
        }
        _intArrayLiteral = null;
        _floatArrayLiteral = null;
        _doubleArrayLiteral = null;
        break;
      case FLOAT:
        float startFloat = Float.parseFloat(literalContexts.get(0).getLiteral().getStringValue());
        float endFloat = Float.parseFloat(literalContexts.get(1).getLiteral().getStringValue());
        float incFloat = Float.parseFloat(literalContexts.get(2).getLiteral().getStringValue());
        size = (int) ((endFloat - startFloat) / incFloat + 1);
        _floatArrayLiteral = new float[size];
        for (int i = 0 ; i < size; i ++, startFloat += incFloat) {
          _floatArrayLiteral[i] = startFloat;
        }
        _intArrayLiteral = null;
        _longArrayLiteral = null;
        _doubleArrayLiteral = null;
        break;
      case DOUBLE:
        double startDouble = Double.parseDouble(literalContexts.get(0).getLiteral().getStringValue());
        double endDouble = Double.parseDouble(literalContexts.get(1).getLiteral().getStringValue());
        double incDouble = Double.parseDouble(literalContexts.get(2).getLiteral().getStringValue());
        size = (int) ((endDouble - startDouble) / incDouble +1);
        _doubleArrayLiteral = new double[size];
        for (int i = 0 ; i < size; i ++, startDouble += incDouble) {
          _doubleArrayLiteral[i] = startDouble;
      }
        _intArrayLiteral = null;
        _longArrayLiteral = null;
        _floatArrayLiteral = null;
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for ArrayGenerateTransformFunction: " + _dataType + ", literal contexts: "
            + Arrays.toString(literalContexts.toArray()));
    }
  }

  public int[] getIntArrayLiteral() {
    return _intArrayLiteral;
  }

  public long[] getLongArrayLiteral() {
    return _longArrayLiteral;
  }

  public float[] getFloatArrayLiteral() {
    return _floatArrayLiteral;
  }

  public double[] getDoubleArrayLiteral() {
    return _doubleArrayLiteral;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {

  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return new TransformResultMetadata(_dataType, false, false);
  }

  @Nullable
  @Override
  public Dictionary getDictionary() {
    return null;
  }

  @Override
  public int[] transformToDictIdsSV(ValueBlock valueBlock) {
    return new UnsupportedOperationException();
  }

  @Override
  public int[][] transformToDictIdsMV(ValueBlock valueBlock) {
    return new UnsupportedOperationException();
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    return new UnsupportedOperationException();
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    return new UnsupportedOperationException();
  }

  @Override
  public float[] transformToFloatValuesSV(ValueBlock valueBlock) {
    return new UnsupportedOperationException();
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    return new UnsupportedOperationException();
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock) {
    return new UnsupportedOperationException();
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    return new UnsupportedOperationException();
  }

  @Override
  public byte[][] transformToBytesValuesSV(ValueBlock valueBlock) {
    return new UnsupportedOperationException();
  }

  @Override
  public int[][] transformToIntValuesMV(ValueBlock valueBlock) {
    return new int[0][];
  }

  @Override
  public long[][] transformToLongValuesMV(ValueBlock valueBlock) {
    return new long[0][];
  }

  @Override
  public float[][] transformToFloatValuesMV(ValueBlock valueBlock) {
    return new float[0][];
  }

  @Override
  public double[][] transformToDoubleValuesMV(ValueBlock valueBlock) {
    return new double[0][];
  }

  @Override
  public String[][] transformToStringValuesMV(ValueBlock valueBlock) {
    return new String[0][];
  }

  @Override
  public byte[][][] transformToBytesValuesMV(ValueBlock valueBlock) {
    return new byte[0][][];
  }

  @Nullable
  @Override
  public RoaringBitmap getNullBitmap(ValueBlock block) {
    return null;
  }
}
