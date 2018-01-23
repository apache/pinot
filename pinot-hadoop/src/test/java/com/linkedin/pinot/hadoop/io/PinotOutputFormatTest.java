package com.linkedin.pinot.hadoop.io;

import com.linkedin.pinot.common.data.Schema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PinotOutputFormatTest {

    private TaskAttemptContext fakeTaskAttemptContext;
    private Job job;
    private Configuration conf;
    PinotOutputFormat outputFormat;

    @BeforeClass
    public void setUp() throws IOException {
        conf = new Configuration();
        job = Job.getInstance(conf);
        fakeTaskAttemptContext = mock(TaskAttemptContext.class);
        outputFormat = new PinotOutputFormat();
        File outputTempDir = Files.createTempDirectory(PinotOutputFormatTest.class.getName() + "_io_output").toFile();
        File workingTempDir = Files.createTempDirectory(PinotOutputFormatTest.class.getName() + "_io_working_dir").toFile();
        // output path
        Path outDir = new Path(outputTempDir.getAbsolutePath());
        PinotOutputFormat.setOutputPath(job, outDir);
        // file format
        PinotOutputFormat.setFileFormat(job, "json");
        PinotOutputFormat.setTableName(job, "emp");
        PinotOutputFormat.setSegmentName(job, "segment_one");
        PinotOutputFormat.setTempSegmentDir(job, workingTempDir.getAbsolutePath());
        Schema schema = Schema.fromString(getSchema());
        PinotOutputFormat.setSchema(job, schema);
        PinotOutputFormat.setNumThreads(job, 10);
        mockTaskAttemptContext();
    }

    private void mockTaskAttemptContext() {
        TaskAttemptID fakeTaskId = new TaskAttemptID(new TaskID("foo_task", 123, TaskType.REDUCE, 2), 2);
        when(fakeTaskAttemptContext.getTaskAttemptID())
                .thenReturn(fakeTaskId);
        when(fakeTaskAttemptContext.getConfiguration()).thenReturn(job.getConfiguration());
    }


    @Test
    public void testMe() throws Exception {
        int days = 2000;
        int sal = 20;
        RecordWriter<Object, byte[]> writer = outputFormat.getRecordWriter(fakeTaskAttemptContext);
        for (int i = 0; i < 10000; i++) {
            String name = "name " + i;
            Emp e = new Emp(i, name, days + i, sal + i);
            writer.write(null, e.toString().getBytes());
        }
        writer.close(fakeTaskAttemptContext);
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
