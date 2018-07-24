package com.linkedin.pinot.core.startreeV2;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.data.readers.PinotSegmentColumnReader;


public class StarTreeV2BuilderHelper {

  /**
   * Helper function to read value of a doc in a column
   *
   * @param reader 'PinotSegmentColumnReader' to read data.
   * @param dataType 'FieldSpec.DataType' of the data to be read.
   * @param docId 'int' doc id to be read.
   *
   * @return Object
   */
  public static Object readHelper(PinotSegmentColumnReader reader, FieldSpec.DataType dataType, int docId) {
    switch (dataType) {
      case INT:
        return reader.readInt(docId);
      case FLOAT:
        return reader.readFloat(docId);
      case LONG:
        return reader.readLong(docId);
      case DOUBLE:
        return reader.readDouble(docId);
      case STRING:
        return reader.readString(docId);
    }

    return null;
  }
}
