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
package com.linkedin.pinot.integration.tests;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;

@Test
public class NoDictionaryIntegrationTest extends OfflineClusterIntegrationTest {

  @Override
  protected List<String> getRawIndexColumns() {
    try {
      Schema schema = Schema.fromFile(getSchemaFile());

      List<String> noDictionaryColumns = new ArrayList<>();
      for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
        //as of now, we only support no dictionary on single value columns
        if (fieldSpec.isSingleValueField()) {
          noDictionaryColumns.add(fieldSpec.getName());
        }
      }
      return noDictionaryColumns;
    } catch (Exception e) {
      throw new RuntimeException("Exception computing the no dictionary columns", e);
    }
  }
}
