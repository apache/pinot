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
package org.apache.pinot.controller.recommender.data.writer;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.controller.recommender.data.generator.DataGenerator;


public class CsvWriter extends FileWriter {
  @Override
  protected String generateRow(DataGenerator generator) {
    Map<String, Object> row = generator.nextRow();
    int colCount = row.size();
    Object[] values = new Object[colCount];
    int index = 0;
    for (String key : row.keySet()) {
      values[index] = serializeIfMultiValue(row.get(key));
      index++;
    }
    return StringUtils.join(values, ",");
  }

  @Override
  public void write()
      throws Exception {
    super.write();
    final String headers = StringUtils.join(_spec.getGenerator().nextRow().keySet(), ",");
    for (File file : Objects.requireNonNull(_spec.getBaseDir().listFiles())) {
      File tempFile = new File(_spec.getBaseDir(), String.format("temp_%s", file.getName()));
      try (java.io.FileWriter writer = new java.io.FileWriter(tempFile)) {
        writer.append(headers).append('\n');
        try (FileReader reader = new FileReader(file)) {
          reader.transferTo(writer);
        }
      }
      Preconditions.checkState(file.delete());
      Preconditions.checkState(tempFile.renameTo(file));
    }
  }

  private Object serializeIfMultiValue(Object obj) {
    if (obj instanceof List) {
      return StringUtils.join((List) obj, ";");
    }
    return obj;
  }

  @Override
  protected String getExtension() {
    return "csv";
  }
}
