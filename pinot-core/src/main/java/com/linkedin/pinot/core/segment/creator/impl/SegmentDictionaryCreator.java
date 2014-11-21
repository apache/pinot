package com.linkedin.pinot.core.segment.creator.impl;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.index.reader.impl.FixedByteWidthRowColDataFileReader;
import com.linkedin.pinot.core.index.writer.impl.FixedByteWidthRowColDataFileWriter;
import com.linkedin.pinot.core.indexsegment.utils.ByteBufferBinarySearchUtil;


public class SegmentDictionaryCreator {
  private final boolean hasNulls;
  private final Object[] sortedList;
  private final FieldSpec spec;
  private final File dictionaryFile;
  private FixedByteWidthRowColDataFileReader dataReader;
  private ByteBufferBinarySearchUtil searchableByteBuffer;
  private int stringColumnMaxLength = 0;

  public SegmentDictionaryCreator(boolean hasNulls, Object[] sortedList, FieldSpec spec, File indexDir) throws IOException {
    this.hasNulls = hasNulls;
    this.sortedList = sortedList;
    this.spec = spec;
    dictionaryFile = new File(indexDir, spec.getName() + ".dict");
    FileUtils.touch(dictionaryFile);
  }

  public void build() throws Exception {
    switch (spec.getDataType()) {
      case INT:
        final FixedByteWidthRowColDataFileWriter intDictionaryWrite =
        new FixedByteWidthRowColDataFileWriter(dictionaryFile, sortedList.length, 1, V1Constants.Dict.INT_DICTIONARY_COL_SIZE);
        for (int i = 0; i < sortedList.length; i++) {
          final int entry = ((Integer) sortedList[i]).intValue();
          intDictionaryWrite.setInt(i, 0, entry);
        }
        intDictionaryWrite.saveAndClose();

        dataReader =
            FixedByteWidthRowColDataFileReader.forMmap(dictionaryFile, sortedList.length, 1, V1Constants.Dict.INT_DICTIONARY_COL_SIZE);
        break;
      case FLOAT:
        final FixedByteWidthRowColDataFileWriter floatDictionaryWrite =
        new FixedByteWidthRowColDataFileWriter(dictionaryFile, sortedList.length, 1, V1Constants.Dict.FOLAT_DICTIONARY_COL_SIZE);
        for (int i = 0; i < sortedList.length; i++) {
          final float entry = ((Float) sortedList[i]).floatValue();
          floatDictionaryWrite.setFloat(i, 0, entry);
        }
        floatDictionaryWrite.saveAndClose();
        dataReader =
            FixedByteWidthRowColDataFileReader.forMmap(dictionaryFile, sortedList.length, 1, V1Constants.Dict.FOLAT_DICTIONARY_COL_SIZE);
        break;
      case LONG:
        final FixedByteWidthRowColDataFileWriter longDictionaryWrite =
        new FixedByteWidthRowColDataFileWriter(dictionaryFile, sortedList.length, 1, V1Constants.Dict.LONG_DICTIONARY_COL_SIZE);
        for (int i = 0; i < sortedList.length; i++) {
          final long entry = ((Long) sortedList[i]).longValue();
          longDictionaryWrite.setLong(i, 0, entry);
        }
        longDictionaryWrite.saveAndClose();
        dataReader =
            FixedByteWidthRowColDataFileReader.forMmap(dictionaryFile, sortedList.length, 1, V1Constants.Dict.LONG_DICTIONARY_COL_SIZE);
        break;
      case DOUBLE:
        final FixedByteWidthRowColDataFileWriter doubleDictionaryWrite =
        new FixedByteWidthRowColDataFileWriter(dictionaryFile, sortedList.length, 1, V1Constants.Dict.DOUBLE_DICTIONARY_COL_SIZE);
        for (int i = 0; i < sortedList.length; i++) {
          final double entry = ((Double) sortedList[i]).doubleValue();
          doubleDictionaryWrite.setDouble(i, 0, entry);
        }
        doubleDictionaryWrite.saveAndClose();
        dataReader =
            FixedByteWidthRowColDataFileReader.forMmap(dictionaryFile, sortedList.length, 1, V1Constants.Dict.DOUBLE_DICTIONARY_COL_SIZE);
        break;
      case STRING:
      case BOOLEAN:
        for (final Object e : sortedList) {
          if (stringColumnMaxLength < ((String) e).length()) {
            stringColumnMaxLength = ((String) e).length();
          }
        }

        final FixedByteWidthRowColDataFileWriter stringDictionaryWrite =
            new FixedByteWidthRowColDataFileWriter(dictionaryFile, sortedList.length, 1, new int[] { stringColumnMaxLength });

        final String[] revised = new String[sortedList.length];
        for (int i = 0; i < sortedList.length; i++) {
          final String toWrite = sortedList[i].toString();
          final int padding = stringColumnMaxLength - toWrite.length();

          final StringBuilder bld = new StringBuilder();
          for (int j = 0; j < padding; j++) {
            bld.append(V1Constants.Str.STRING_PAD_CHAR);
          }
          bld.append(toWrite);
          revised[i] = bld.toString();
        }
        Arrays.sort(revised);

        for (int i = 0; i < revised.length; i++) {
          stringDictionaryWrite.setString(i, 0, revised[i]);
        }
        stringDictionaryWrite.saveAndClose();
        dataReader = FixedByteWidthRowColDataFileReader.forMmap(dictionaryFile, sortedList.length, 1, new int[] { stringColumnMaxLength });
        break;
      default:
        break;
    }
    searchableByteBuffer = new ByteBufferBinarySearchUtil(dataReader);
  }

  public int getStringColumnMaxLength() {
    return stringColumnMaxLength;
  }

  public Object indexOf(Object e) {
    if (spec.isSingleValueField()) {
      return indexOfSV(e);
    } else {
      return indexOfMV(e);
    }
  }

  private Integer indexOfSV(Object e) {

    if (e == null) {
      return new Integer(-1);
    }

    switch (spec.getDataType()) {
      case INT:
        final int intValue = ((Integer) e).intValue();
        return new Integer(searchableByteBuffer.binarySearch(0, intValue));
      case FLOAT:
        final float floatValue = ((Float) e).floatValue();
        return new Integer(searchableByteBuffer.binarySearch(0, floatValue));
      case DOUBLE:
        final double doubleValue = ((Double) e).doubleValue();
        return new Integer(searchableByteBuffer.binarySearch(0, doubleValue));
      case LONG:
        final long longValue = ((Long) e).longValue();
        return new Integer(searchableByteBuffer.binarySearch(0, longValue));
      case STRING:
      case BOOLEAN:
        final StringBuilder bld = new StringBuilder();
        for (int i = 0; i < (stringColumnMaxLength - ((String) e).length()); i++) {
          bld.append(V1Constants.Str.STRING_PAD_CHAR);
        }
        bld.append(e.toString());
        return new Integer(searchableByteBuffer.binarySearch(0, bld.toString()));
      default:
        break;
    }

    throw new UnsupportedOperationException("unsupported data type : " + spec.getDataType() + " : " + " for column : " + spec.getName());
  }

  private Integer[] indexOfMV(Object e) {
    if (e == null) {
      return new Integer[] { -1 };
    }

    final Object[] multiValues = (Object[]) e;
    final Integer[] ret = new Integer[multiValues.length];

    switch (spec.getDataType()) {
      case INT:
        for (int i = 0; i < multiValues.length; i++) {
          ret[i] = searchableByteBuffer.binarySearch(0, ((Integer) multiValues[i]).intValue());
        }
        break;
      case FLOAT:
        for (int i = 0; i < multiValues.length; i++) {
          ret[i] = searchableByteBuffer.binarySearch(0, ((Float) multiValues[i]).floatValue());
        }
        break;
      case LONG:
        for (int i = 0; i < multiValues.length; i++) {
          ret[i] = searchableByteBuffer.binarySearch(0, ((Long) multiValues[i]).longValue());
        }
        break;
      case DOUBLE:
        for (int i = 0; i < multiValues.length; i++) {
          ret[i] = searchableByteBuffer.binarySearch(0, ((Double) multiValues[i]).doubleValue());
        }
        break;
      case STRING:
      case BOOLEAN:
        final StringBuilder bld = new StringBuilder();
        for (int i = 0; i < multiValues.length; i++) {
          for (int j = 0; j < (stringColumnMaxLength - ((String) e).length()); j++) {
            bld.append(V1Constants.Str.STRING_PAD_CHAR);
          }
          bld.append(multiValues[i].toString());
          ret[i] = searchableByteBuffer.binarySearch(0, bld.toString());
        }
        break;
      default:
        break;
    }

    return ret;
  }
}
