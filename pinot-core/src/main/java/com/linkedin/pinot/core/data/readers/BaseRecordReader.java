/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.data.Schema;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableLong;

public abstract class BaseRecordReader implements RecordReader {
  private Map<String, MutableLong> _nullCountMap;

  protected BaseRecordReader() {

  }

  protected  void initNullCounters(Schema schema) {
    _nullCountMap = new HashMap<String, MutableLong>(schema.getAllFieldSpecs().size());
    for (String fieldName : schema.getColumnNames()) {
      _nullCountMap.put(fieldName, new MutableLong(0));
    }
  }

  protected void incrementNullCountFor(String fieldName) {
    _nullCountMap.get(fieldName).increment();
  }

  @Override
  public Map<String, MutableLong> getNullCountMap() {
    return _nullCountMap;
  }

}
