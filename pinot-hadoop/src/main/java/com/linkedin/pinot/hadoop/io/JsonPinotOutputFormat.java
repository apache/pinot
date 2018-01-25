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

import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;

public class JsonPinotOutputFormat<K, V extends Serializable> extends PinotOutputFormat<K, V> {

    @Override
    public void configure(Configuration conf) {
        conf.set(PinotOutputFormat.DATA_WRITE_SUPPORT_CLASS, JsonDataWriteSupport.class.getName());
        conf.set(PinotOutputFormat.FILE_FORMAT, FileFormat.JSON.name());
    }

    public static class JsonDataWriteSupport<V> implements DataWriteSupport<V> {

        ObjectMapper _mapper;

        @Override
        public void init(SegmentGeneratorConfig segmentConfig, TaskAttemptContext context) {
            _mapper = new ObjectMapper();
        }

        @Override
        public byte[] write(V v) {
            try {
                return _mapper.writeValueAsBytes(v);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close(TaskAttemptContext context) {

        }
    }

}
