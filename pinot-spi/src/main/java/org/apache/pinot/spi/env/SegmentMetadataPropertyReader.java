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

import java.io.Reader;
import org.apache.commons.configuration2.PropertiesConfiguration.PropertiesReader;


class SegmentMetadataPropertyReader extends PropertiesReader {
  private final boolean _skipUnescapePropertyName;

  public SegmentMetadataPropertyReader(Reader reader, boolean skipUnescapePropertyName) {
    super(reader);
    _skipUnescapePropertyName = skipUnescapePropertyName;
  }

  @Override
  protected void parseProperty(final String line) {
    if (_skipUnescapePropertyName) {
      String regex = "[" + getPropertySeparator() + "]";
      String[] keyValue = line.split(regex);
      initPropertyName(keyValue[0]);
      initPropertyValue(keyValue[1]);
      initPropertySeparator(regex);
    } else {
      super.parseProperty(line);
    }
  }

  @Override
  protected String unescapePropertyName(final String name) {
    if (_skipUnescapePropertyName) {
      return name;
    }
    return super.unescapePropertyName(name);
  }
}
