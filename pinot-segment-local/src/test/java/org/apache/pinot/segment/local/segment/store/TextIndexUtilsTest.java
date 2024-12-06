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
package org.apache.pinot.segment.local.segment.store;

import java.io.File;
import java.util.Arrays;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.text.TextIndexConfigBuilder;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class TextIndexUtilsTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "TextIndexUtilsTest");

  @Test
  public void testRoundTripProperties()
      throws Exception {
    TextIndexConfig config =
        new TextIndexConfigBuilder().withLuceneAnalyzerClass("org.apache.lucene.analysis.core.KeywordAnalyzer")
            .withLuceneAnalyzerClassArgs(
                Arrays.asList(" \\,.\n\t()[]{}\"':=-_$\\?@&|#+/", "\\,.()[]{}\"':=-_$\\?@&|#+"))
            .withLuceneAnalyzerClassArgTypes(Arrays.asList("java.lang.String", "java.lang.String"))
            .withLuceneQueryParserClass("org.apache.pinot.utils.lucene.queryparser.FakeQueryParser").build();

    TextIndexUtils.writeConfigToPropertiesFile(TEMP_DIR, config);
    TextIndexConfig readConfig = TextIndexUtils.getUpdatedConfigFromPropertiesFile(
        new File(TEMP_DIR, V1Constants.Indexes.LUCENE_TEXT_INDEX_PROPERTIES_FILE),
        JsonUtils.stringToObject("{}", TextIndexConfig.class));
    assertEquals(readConfig, config);
  }
}
