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

import com.google.common.base.Preconditions;
import java.io.Reader;
import org.apache.commons.configuration2.PropertiesConfiguration.PropertiesReader;


/**
 * SegmentMetadataPropertyReader extends the PropertiesReader
 * <p>
 * Purpose: loads the segment metadata faster
 *  - by skipping the unescaping of key and
 *  - parsing the line by splitting based on first occurrence of separator
 */
class SegmentMetadataPropertyReader extends PropertiesReader {
  private final boolean _skipUnescapePropertyName;

  public SegmentMetadataPropertyReader(Reader reader, boolean skipUnescapePropertyName) {
    super(reader);
    _skipUnescapePropertyName = skipUnescapePropertyName;
  }

  @Override
  protected void parseProperty(final String line) {
    // if newer version of the segment metadata(based on version value in the property configuration header)
    // skip the regex based parsing of the line content and splitting the content based on first occurrence of separator
    if (_skipUnescapePropertyName) {
      String regex = "[" + getPropertySeparator() + "]";
      String[] keyValue = line.split(regex);
      Preconditions.checkArgument(keyValue.length != 2, "property content split should result in key and value");
      initPropertyName(keyValue[0]);
      initPropertyValue(keyValue[1]);
      initPropertySeparator(regex);
    } else {
      // for backward compatability, follow the default approach
      super.parseProperty(line);
    }
  }

  @Override
  protected String unescapePropertyName(final String name) {
    // skip the unescaping of the propertyName(key), if newer version of the segment metadata.
    if (_skipUnescapePropertyName) {
      return name;
    }
    return super.unescapePropertyName(name);
  }
}
