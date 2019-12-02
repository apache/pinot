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

import java.io.IOException;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReaderWrapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;


/**
 * Create Hadoop splits across files.
 * By default, hadoop cannot create splits across files. As a result, if the input has lots of small avro files,
 * the job will end up having lots of short running mappers, which is inefficient.
 * This class allows creating splits across avro files so we can have larger splits and fewer long running mappers,
 * which improves the performance of the Hadoop job.
 */
public class CombineAvroKeyInputFormat<T> extends CombineFileInputFormat<AvroKey<T>, NullWritable> {
  @Override
  public RecordReader<AvroKey<T>, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException {
    Class cls = AvroKeyRecordReaderWrapper.class;

    return new CombineFileRecordReader<>((CombineFileSplit) split, context,
        (Class<? extends RecordReader<AvroKey<T>, NullWritable>>) cls);
  }

  public static class AvroKeyRecordReaderWrapper<T> extends CombineFileRecordReaderWrapper<AvroKey<T>, NullWritable> {
    public AvroKeyRecordReaderWrapper(CombineFileSplit split, TaskAttemptContext context, Integer index)
        throws IOException, InterruptedException {
      super(new AvroKeyInputFormat<>(), split, context, index);
    }
  }
}
