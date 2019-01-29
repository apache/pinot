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
package org.apache.pinot.hadoop.io;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.Serializable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.utils.JsonUtils;


/**
 * OutputFormat implementation for Json source
 */
public class JsonPinotOutputFormat<K, V extends Serializable> extends PinotOutputFormat<K, V> {
  private static final String JSON_READER_CLASS = "json.reader.class";

  @Override
  public void configure(Configuration conf) {
    conf.set(PinotOutputFormat.PINOT_RECORD_SERIALIZATION_CLASS, JsonPinotRecordSerialization.class.getName());
  }

  public static void setJsonReaderClass(JobContext context, Class<?> clazz) {
    context.getConfiguration().set(JSON_READER_CLASS, clazz.getName());
  }

  public static String getJsonReaderClass(Configuration conf) {
    if (conf.get(JSON_READER_CLASS) == null) {
      throw new RuntimeException("Json reader class not set");
    }
    return conf.get(JSON_READER_CLASS);
  }

  public static class JsonPinotRecordSerialization<T> implements PinotRecordSerialization<T> {
    private Schema _schema;
    private Configuration _conf;
    private PinotRecord _record;

    @Override
    public void init(Configuration conf, Schema schema) {
      _schema = schema;
      _conf = conf;
      _record = new PinotRecord(_schema);
    }

    @Override
    public PinotRecord serialize(T t) {
      _record.clear();
      JsonNode jsonRecord = JsonUtils.objectToJsonNode(t);
      for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
        String column = fieldSpec.getName();
        _record.putField(column, JsonUtils.extractValue(jsonRecord.get(column), fieldSpec));
      }
      return _record;
    }

    @Override
    public T deserialize(PinotRecord record)
        throws IOException {
      ObjectNode jsonRecord = JsonUtils.newObjectNode();
      for (String column : _schema.getColumnNames()) {
        jsonRecord.set(column, JsonUtils.objectToJsonNode(record.getValue(column)));
      }
      return JsonUtils.jsonNodeToObject(jsonRecord, getJsonReaderClass(_conf));
    }

    @Override
    public void close() {
    }

    private Class<T> getJsonReaderClass(Configuration conf) {
      try {
        return (Class<T>) Class.forName(JsonPinotOutputFormat.getJsonReaderClass(conf));
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Error initialize json reader class", e);
      }
    }
  }
}
