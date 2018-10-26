/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.readers;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code LimitStringLengthRecordReader} class is the record reader that limits the length of the string fields
 * based on the limit set in the schema field specs.
 */
public class LimitStringLengthRecordReader implements RecordReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(LimitStringLengthRecordReader.class);

  private final RecordReader _recordReader;
  private final Map<String, Integer> _lengthLimits = new HashMap<>();

  private long _numStringsTrimmed;

  public LimitStringLengthRecordReader(RecordReader recordReader) {
    _recordReader = recordReader;
    for (FieldSpec fieldSpec : recordReader.getSchema().getAllFieldSpecs()) {
      if (fieldSpec.getDataType() == FieldSpec.DataType.STRING) {
        int lengthLimit = fieldSpec.getStringLengthLimit();
        if (lengthLimit > 0) {
          _lengthLimits.put(fieldSpec.getName(), lengthLimit);
          LOGGER.info("Limiting length of column: {} to: {}", fieldSpec.getName(), lengthLimit);
        }
      }
    }
  }

  public long getNumStringsTrimmed() {
    return _numStringsTrimmed;
  }

  @Override
  public boolean hasNext() {
    return _recordReader.hasNext();
  }

  @Override
  public GenericRow next() throws IOException {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse) throws IOException {
    GenericRow next = _recordReader.next(reuse);
    for (Map.Entry<String, Integer> entry : _lengthLimits.entrySet()) {
      String column = entry.getKey();
      int lengthLimit = entry.getValue();

      // The inner record reader should already replace missing value with default null value
      Object value = next.getValue(column);
      assert value != null;

      if (value instanceof String) {
        // Single-valued column
        String stringValue = (String) value;
        if (stringValue.length() > lengthLimit) {
          next.putField(column, stringValue.substring(0, lengthLimit));
          _numStringsTrimmed++;
        }
      } else {
        // Multi-valued column
        assert value instanceof Object[];
        Object[] values = (Object[]) value;
        int length = values.length;
        for (int i = 0; i < length; i++) {
          assert values[i] instanceof String;
          String stringValue = (String) values[i];
          if (stringValue.length() > lengthLimit) {
            values[i] = stringValue.substring(0, lengthLimit);
            _numStringsTrimmed++;
          }
        }
      }
    }
    return next;
  }

  @Override
  public void rewind() throws IOException {
    _recordReader.rewind();
  }

  @Override
  public Schema getSchema() {
    return _recordReader.getSchema();
  }

  @Override
  public void close() throws IOException {
    _recordReader.close();

    if (_numStringsTrimmed > 0) {
      LOGGER.warn("Number of strings trimmed: {}", _numStringsTrimmed);
    }
  }
}
