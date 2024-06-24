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
package org.apache.pinot.spi.env;

import java.io.Writer;
import org.apache.commons.configuration2.PropertiesConfiguration.PropertiesWriter;
import org.apache.commons.configuration2.convert.ListDelimiterHandler;


/**
 * SegmentMetadataPropertyWriter extends the PropertiesWriter
 * <p>
 * Purpose: custom property writer for writing the segment metadata faster by skipping the escaping of key.
 */
public class VersionedPropertyWriter extends PropertiesWriter {

  public VersionedPropertyWriter(final Writer writer, ListDelimiterHandler handler) {
    super(writer, handler);
  }

  @Override
  protected String escapeKey(final String key) {
    // skip the escapeKey functionality,
    return key;
  }
}
