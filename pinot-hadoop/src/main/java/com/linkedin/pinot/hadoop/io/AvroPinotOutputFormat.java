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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class AvroPinotOutputFormat<K> extends PinotOutputFormat<K, GenericData.Record> {

    @Override
    public void configure(Configuration conf) {
        super.configure(conf);
    }

    public static class AvroPinotSerialization implements PinotRecordSerialization<GenericData.Record> {

        @Override
        public void init(Configuration conf, Schema schema) {

        }

        @Override
        public PinotRecord serialize(GenericData.Record record) throws IOException {
            return new PinotRecord(record);
        }

        @Override
        public GenericData.Record deserialize(PinotRecord record) throws IOException {
            return record.get();
        }

        @Override
        public void close() {
        }
    }
}
