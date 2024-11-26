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
import org.apache.commons.lang3.StringUtils;


/**
 * VersionedPropertyReader extends the PropertiesReader
 * <p>
 * Purpose: loads the segment metadata faster
 *  - by skipping the unescaping of key and
 *  - parsing the line by splitting based on first occurrence of separator
 */
class VersionedPropertyReader extends PropertiesReader {

  public VersionedPropertyReader(Reader reader) {
    super(reader);
  }

  @Override
  protected void parseProperty(final String line) {
    // skip the regex based parsing of the line content and splitting the content based on first occurrence of separator
    // getPropertySeparator(), in general returns the PropertiesConfiguration `DEFAULT_SEPARATOR` value i.e. ' = '.
    String separator = getPropertySeparator();
    Preconditions.checkArgument(CommonsConfigurationUtils.VERSIONED_CONFIG_SEPARATOR.equals(separator),
        "Versioned property configuration separator '" + separator + "' should be equal to '"
            + CommonsConfigurationUtils.VERSIONED_CONFIG_SEPARATOR + "'");

    String[] keyValue = StringUtils.splitByWholeSeparator(line, separator, 2);
    Preconditions.checkArgument(keyValue.length == 2, "property content split should result in key and value");
    initPropertyName(keyValue[0]);
    initPropertyValue(keyValue[1]);
    initPropertySeparator(separator);
  }

  @Override
  protected String unescapePropertyName(final String name) {
    // skip the unescaping of the propertyName(key)
    return name;
  }
}
