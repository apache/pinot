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
import com.linkedin.pinot.core.data.GenericRow;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableLong;


/**
 * Generic interface to implement any new file format in which
 * input data can be read and converted into segments.
 *
 *
 */

public interface RecordReader {

  /**
   *
   * @throws Exception
   */
  public void init() throws Exception;

  /**
   * Rewind is called in case we need to iterate through
   * the input data again.. Once such case would be when
   * we need to create a dictionary. Cannot call this if
   * close is called first
   *
   * @throws Exception
   */
  public void rewind() throws Exception;

  /**
   *
   * @return
   */
  public boolean hasNext();

  /**
   *
   * @return
   */
  public Schema getSchema();

  /**
   *
   * @return
   */
  public GenericRow next();

  /**
   *
   * @return
   */
  public GenericRow next(GenericRow row);

  /**
   * Get the map of fields that have null values.
   */
  public Map<String, MutableLong> getNullCountMap();

  /**
   *
   * @throws Exception
   */
  public void close() throws Exception;
}
