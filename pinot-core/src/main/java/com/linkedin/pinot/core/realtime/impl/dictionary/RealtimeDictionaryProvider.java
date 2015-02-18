package com.linkedin.pinot.core.realtime.impl.dictionary;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;

public class RealtimeDictionaryProvider {

  public static MutableDictionaryReader getDictionaryFor(FieldSpec spec) {
    switch (spec.getDataType()) {
      case INT:
        return new IntMutableDictionary(spec);
      case LONG:
        return new LongMutableDictionary(spec);
      case FLOAT:
        return new FloatMutableDictionary(spec);
      case DOUBLE:
        return new DoubleMutableDictionary(spec);
      case BOOLEAN:
      case STRING:
        return new StringMutableDictionary(spec);
    }
    throw new UnsupportedOperationException();
  }
}
