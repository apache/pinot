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
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * OutputFormat implementation for Json source
 * @param <K>
 * @param <V>
 */
public class JsonPinotOutputFormat<K, V extends Serializable> extends PinotOutputFormat<K, V> {

    public static final String JSON_READER_CLASS = "json.reader.class";

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

        private ObjectMapper _mapper;
        private Schema _schema;
        private DatumReader<GenericData.Record> _reader;
        private DatumWriter<GenericRecord> _writer;
        private Configuration _conf;
        private ByteArrayOutputStream _outputStream;

        @Override
        public void init(Configuration conf, Schema schema) {
            _schema = schema;
            _conf = conf;
            _reader = new GenericDatumReader(_schema);
            _writer = new GenericDatumWriter(_schema);
            _mapper = new ObjectMapper();
            _outputStream = new ByteArrayOutputStream();
        }

        @Override
        public PinotRecord serialize(T t) throws IOException {
            Decoder decoder = DecoderFactory.get().jsonDecoder(_schema, _mapper.writeValueAsString(t));
            return new PinotRecord(_reader.read(null, decoder));
        }

        @Override
        public T deserialize(PinotRecord record) throws IOException {
            _outputStream.reset();
            JsonEncoder encoder = EncoderFactory.get().jsonEncoder(_schema, _outputStream);
            _writer.write(record.get(), encoder);
            encoder.flush();
            return _mapper.readValue(_outputStream.toByteArray(), initReader(_conf));
        }

        @Override
        public void close() {

        }

        private Class<T> initReader(Configuration conf) {
            try {
                return (Class<T>) Class.forName(getJsonReaderClass(conf));
            } catch (Exception e) {
                throw new RuntimeException("Error initializing json reader class", e);
            }
        }


    }

}
