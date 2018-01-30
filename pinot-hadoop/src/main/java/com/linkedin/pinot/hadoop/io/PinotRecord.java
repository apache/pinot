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
package com.linkedin.pinot.hadoop.io;

import com.linkedin.pinot.core.util.AvroUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Container Object for {@link PinotOutputFormat}
 */
public class PinotRecord implements GenericRecord {

    private GenericData.Record _record;
    private Schema _avroSchema;
    private com.linkedin.pinot.common.data.Schema _pinotSchema;

    public PinotRecord(com.linkedin.pinot.common.data.Schema pinotSchema) {
        _pinotSchema = pinotSchema;
        _avroSchema = AvroUtils.getAvroSchemaFromPinotSchema(_pinotSchema);
        _record = new GenericData.Record(_avroSchema);
    }

    public PinotRecord(GenericData.Record record) {
        _record = record;
        _avroSchema = record.getSchema();
    }

    @Override
    public void put(String key, Object v) {
        _record.put(key, v);
    }

    @Override
    public Object get(String key) {
        return _record.get(key);
    }

    @Override
    public void put(int i, Object v) {
        _record.put(i, v);
    }

    @Override
    public Object get(int i) {
        return _record.get(i);
    }

    @Override
    public Schema getSchema() {
        return _avroSchema;
    }

    public GenericData.Record get() {
        return _record;
    }

}
