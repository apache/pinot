/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.hadoop.io;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.Serializable;

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

        private PinotRecord _record;
        private Schema _schema;
        private ObjectMapper _mapper;
        private Configuration _conf;

        @Override
        public void init(Configuration conf, Schema schema) {
            _schema = schema;
            _mapper = new ObjectMapper();
            _conf = conf;
        }

        @Override
        public PinotRecord serialize(T t) throws IOException {
            _record = PinotRecord.createOrReuseRecord(_record, _schema);
            try {
                JSONObject obj = new JSONObject(_mapper.writeValueAsString(t));
                for (FieldSpec fieldSpec : _record.getSchema().getAllFieldSpecs()) {
                    Object fieldValue = getFieldValue(obj, fieldSpec);
                    _record.putField(fieldSpec.getName(), fieldValue);
                }
            } catch (JSONException e) {
                throw new RuntimeException("Serialization exception", e);
            }
            return _record;
        }

        @Override
        public T deserialize(PinotRecord record) throws IOException {
            JSONObject obj = new JSONObject();
            for (FieldSpec fieldSpec : _record.getSchema().getAllFieldSpecs()) {
                Object value = record.getValue(fieldSpec.getName());
                addJsonField(obj, fieldSpec, value);
            }
            return _mapper.readValue(obj.toString().getBytes("UTF-8"), getJsonReaderClass(_conf));
        }

        private void addJsonField(JSONObject obj, FieldSpec fieldSpec, Object value) {
            try {
                if (value instanceof Object[]) {
                    obj.put(fieldSpec.getName(), new JSONArray(value));
                } else {
                    obj.put(fieldSpec.getName(), value);
                }
            } catch (JSONException e) {
                throw new RuntimeException("Error initialize json reader class", e);
            }
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

        private Object getFieldValue(JSONObject obj, FieldSpec fieldSpec) {
            Object fieldValue = obj.opt(fieldSpec.getName());
            if (fieldValue != null) {
                return fieldValue;
            }
            return fieldSpec.getDefaultNullValue();
        }
    }

}
