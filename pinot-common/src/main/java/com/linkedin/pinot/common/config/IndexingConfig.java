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
package com.linkedin.pinot.common.config;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonIgnoreProperties(ignoreUnknown = true)
public class IndexingConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(IndexingConfig.class);

  private List<String> invertedIndexColumns;
  private List<String> sortedColumn = new ArrayList<String>();
  private String loadMode;
  private String lazyLoad;
  private Map<String, String> streamConfigs = new HashMap<String, String>();
  private String segmentFormatVersion;
  private String starTreeFormat;
  private List<String> noDictionaryColumns;

  public IndexingConfig() {

  }

  public List<String> getSortedColumn() {
    return sortedColumn;
  }

  public void setSortedColumn(List<String> sortedColumn) {
    this.sortedColumn = sortedColumn;
  }

  public Map<String, String> getStreamConfigs() {
    return streamConfigs;
  }

  public void setStreamConfigs(Map<String, String> streamConfigs) {
    this.streamConfigs = streamConfigs;
  }

  public List<String> getInvertedIndexColumns() {
    return invertedIndexColumns;
  }

  public void setInvertedIndexColumns(List<String> invertedIndexColumns) {
    this.invertedIndexColumns = invertedIndexColumns;
  }

  public String getLoadMode() {
    return loadMode;
  }

  public void setLoadMode(String loadMode) {
    this.loadMode = loadMode;
  }

  public String getLazyLoad() {
    return lazyLoad;
  }

  public void setLazyLoad(String lazyLoad) {
    this.lazyLoad = lazyLoad;
  }

  public void setSegmentFormatVersion(String segmentFormatVersion) {
    this.segmentFormatVersion = segmentFormatVersion;
  }

  public String getSegmentFormatVersion() {
    return segmentFormatVersion;
  }

  public List<String> getNoDictionaryColumns() {
    return noDictionaryColumns;
  }

  public void setNoDictionaryColumns(List<String> noDictionaryColumns) {
    this.noDictionaryColumns = noDictionaryColumns;
  }

  @Override
  public String toString() {
    final StringBuilder result = new StringBuilder();
    final String newLine = System.getProperty("line.separator");

    result.append(this.getClass().getName());
    result.append(" Object {");
    result.append(newLine);

    //determine fields declared in this class only (no fields of superclass)
    final Field[] fields = this.getClass().getDeclaredFields();

    //print field names paired with their values
    for (final Field field : fields) {
      result.append("  ");
      try {
        result.append(field.getName());
        result.append(": ");
        //requires access to private field:
        result.append(field.get(this));
      } catch (final IllegalAccessException ex) {
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn("Caught exception while processing field " + field, ex);
        }
      }
      result.append(newLine);
    }
    result.append("}");

    return result.toString();
  }

  public String getStarTreeFormat() {
    return starTreeFormat;
  }

  public void setStarTreeFormat(String starTreeFormat) {
    this.starTreeFormat = starTreeFormat;
  }
}
