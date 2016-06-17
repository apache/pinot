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
package com.linkedin.pinot.tools.data.generator;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;


/**
 * Sep 14, 2014
 */

public class AvroWriter implements FileWriter {
  private final File avroFile;
  private final Schema schema;
  private final Map<String, Generator> generatorMap;
  private final DataFileWriter<GenericData.Record> recordWriter;
  private final org.apache.avro.Schema schemaJSON;

  @SuppressWarnings("deprecation")
  public AvroWriter(File baseDir, int index, Map<String, Generator> generatorMap, Schema schema) throws IOException, JSONException {
    avroFile = new File(baseDir, "part-" + index + ".avro");
    this.generatorMap = generatorMap;
    this.schema = schema;

    schemaJSON = org.apache.avro.Schema.parse(getJSONSchema().toString());
    final GenericDatumWriter<GenericData.Record> datum = new GenericDatumWriter<GenericData.Record>(schemaJSON);
    recordWriter = new DataFileWriter<GenericData.Record>(datum);
    recordWriter.create(schemaJSON, avroFile);
  }

  public JSONObject getJSONSchema() throws JSONException {
    final JSONObject ret = new JSONObject();
    ret.put("name", "data_gen_record");
    ret.put("type", "record");

    final JSONArray fields = new JSONArray();

    for (final FieldSpec spec : schema.getAllFieldSpecs()) {
      fields.put(spec.getDataType().toJSONSchemaFor(spec.getName()));
    }

    ret.put("fields", fields);

    return ret;
  }

  public void writeNext() throws IOException {
    final GenericData.Record outRecord = new GenericData.Record(schemaJSON);
    for (final String column : generatorMap.keySet()) {
      outRecord.put(column, generatorMap.get(column).next());
    }
    recordWriter.append(outRecord);
  }

  public void seal() throws IOException {
    recordWriter.close();
  }
}
