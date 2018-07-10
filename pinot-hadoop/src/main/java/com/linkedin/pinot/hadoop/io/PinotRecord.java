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

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;

import java.io.IOException;
import java.util.Collection;

/**
 * Container Object for {@link PinotOutputFormat}
 */
public class PinotRecord extends GenericRow {

    private Schema _pinotSchema;
    private Collection<String> _fieldNames;

    public PinotRecord(Schema schema) {
        _pinotSchema = schema;
        _fieldNames = _pinotSchema.getColumnNames();
    }

    @Override
    public Object getValue(String fieldName) {
        if (!containsField(fieldName)) {
            throw new IllegalArgumentException(String.format("The field name %s not found in the schema", fieldName));
        }
        return super.getValue(fieldName);
    }

    @Override
    public void putField(String key, Object value) {
        if (!containsField(key)) {
            throw new IllegalArgumentException(String.format("The field name %s not found in the schema", key));
        }
        super.putField(key, value);
    }

    @Override
    public byte[] toBytes() throws IOException {
        return super.toBytes();
    }

    public Schema getSchema() {
        return _pinotSchema;
    }

    public static PinotRecord createOrReuseRecord(PinotRecord record, Schema schema) {
        if (record == null) {
            return new PinotRecord(schema);
        } else {
            record.clear();
            return record;
        }
    }

    private boolean containsField(String fieldName) {
        return _fieldNames.contains(fieldName);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
