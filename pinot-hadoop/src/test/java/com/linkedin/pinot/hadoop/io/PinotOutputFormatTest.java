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
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.PinotSegmentRecordReader;
import com.linkedin.pinot.core.data.readers.RecordReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PinotOutputFormatTest {

    private TaskAttemptContext fakeTaskAttemptContext;
    private Job job;
    private Configuration conf;
    private PinotOutputFormat outputFormat;
    private File outputTempDir;
    private String segmentTarPath;

    private void init(String indexType) throws IOException {
        conf = new Configuration();
        job = Job.getInstance(conf);
        fakeTaskAttemptContext = mock(TaskAttemptContext.class);
        outputFormat = new JsonPinotOutputFormat();
        outputTempDir = Files.createTempDirectory(PinotOutputFormatTest.class.getName() + indexType + "_io_output").toFile();
        File workingTempDir = Files.createTempDirectory(PinotOutputFormatTest.class.getName() + indexType + "_io_working_dir").toFile();
        // output path
        Path outDir = new Path(outputTempDir.getAbsolutePath());
        PinotOutputFormat.setOutputPath(job, outDir);
        PinotOutputFormat.setTableName(job, "emp");
        PinotOutputFormat.setSegmentName(job, indexType + "segment_one");
        PinotOutputFormat.setTempSegmentDir(job, workingTempDir.getAbsolutePath());

        Schema schema = Schema.fromString(getSchema());
        PinotOutputFormat.setSchema(job, schema);
        mockTaskAttemptContext(indexType);
        segmentTarPath = "_temporary/0/_temporary/attempt_foo_task_" + indexType + "_0123_r_000002_2/part-r-00002/segmentTar";

    }

    private void mockTaskAttemptContext(String indexType) {
        TaskAttemptID fakeTaskId = new TaskAttemptID(new TaskID("foo_task_" + indexType, 123, TaskType.REDUCE, 2), 2);
        when(fakeTaskAttemptContext.getTaskAttemptID())
                .thenReturn(fakeTaskId);
        when(fakeTaskAttemptContext.getConfiguration()).thenReturn(job.getConfiguration());
    }


    @Test
    public void verifyStarIndex() throws Exception {
        runPinotOutputFormatTest(true, "star");
    }

    @Test
    public void verifyRawIndex() throws Exception {
        runPinotOutputFormatTest(false, "raw");
    }

    private void runPinotOutputFormatTest(boolean isStarTree, String indexType) throws Exception {
        init(indexType);
        if (isStarTree) {
            PinotOutputFormat.setEnableStarTreeIndex(job, true);
        } else {
            PinotOutputFormat.setEnableStarTreeIndex(job, false);
        }
        Map<Integer, Emp> inputMap = addTestData();
        validate(inputMap);
    }

    private void validate(Map<Integer, Emp> inputMap) throws Exception {
        File segmentTarOutput = new File(outputTempDir, segmentTarPath);
        File untarOutput = Files.createTempDirectory(PinotOutputFormatTest.class.getName() + "_segmentUnTar").toFile();
        for (File tarFile : segmentTarOutput.listFiles()) {
            TarGzCompressionUtils.unTar(tarFile, untarOutput);
        }

        File outputDir = new File(untarOutput, PinotOutputFormat.getSegmentName(fakeTaskAttemptContext));
        RecordReader recordReader = new PinotSegmentRecordReader(outputDir, Schema.fromString(getSchema()), null);
        Map<Integer, GenericRow> resultMap = new HashMap<>();
        while (recordReader.hasNext()) {
            GenericRow row = recordReader.next();
            resultMap.put((Integer) row.getValue("id"), row);
        }

        Assert.assertEquals(resultMap.size(), inputMap.size());
        Assert.assertEquals(resultMap.get(8).getValue("name"), inputMap.get(8).name);
    }

    private Map<Integer, Emp> addTestData() throws IOException, InterruptedException {
        int days = 2000;
        int sal = 20;
        RecordWriter<Object, Emp> writer = outputFormat.getRecordWriter(fakeTaskAttemptContext);
        Map<Integer, Emp> inputMap = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            String name = "name " + i;
            Emp e = new Emp(i, name, days + i, sal + i);
            writer.write(null, e);
            inputMap.put(i, e);
        }
        writer.close(fakeTaskAttemptContext);
        return inputMap;
    }


    private static class Emp implements Serializable {

        public int id;
        public String name;
        public int epochDays;
        public int salary;

        public Emp(int id, String name, int epochDays, int salary) {
            this.id = id;
            this.name = name;
            this.epochDays = epochDays;
            this.salary = salary;
        }

        @Override
        public String toString() {
            return "{\"Emp\":{"
                    + "                        \"id\":\"" + id + "\""
                    + ",                         \"name\":\"" + name + "\""
                    + ",                         \"epochDays\":\"" + epochDays + "\""
                    + ",                         \"salary\":\"" + salary + "\""
                    + "}}";
        }
    }

    private String getSchema() {
        return "{\n" +
                "  \"dimensionFieldSpecs\" : [\n" +
                "    {\n" +
                "      \"name\": \"id\",\n" +
                "      \"dataType\" : \"INT\",\n" +
                "      \"delimiter\" : null,\n" +
                "      \"singleValueField\" : true\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"name\",\n" +
                "      \"dataType\" : \"STRING\",\n" +
                "      \"delimiter\" : null,\n" +
                "      \"singleValueField\" : true\n" +
                "    }\n" +
                "  ],\n" +
                "  \"timeFieldSpec\" : {\n" +
                "    \"incomingGranularitySpec\" : {\n" +
                "      \"timeType\" : \"DAYS\",\n" +
                "      \"dataType\" : \"INT\",\n" +
                "      \"name\" : \"epochDays\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"metricFieldSpecs\" : [\n" +
                "    {\n" +
                "      \"name\" : \"salary\",\n" +
                "      \"dataType\" : \"INT\",\n" +
                "      \"delimiter\" : null,\n" +
                "      \"singleValueField\" : true\n" +
                "    }\n" +
                "   ],\n" +
                "  \"schemaName\" : \"emp\"\n" +
                "}";
    }
}
