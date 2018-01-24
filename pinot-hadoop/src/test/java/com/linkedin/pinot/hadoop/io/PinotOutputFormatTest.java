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
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
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

    @BeforeClass
    public void setUp() throws IOException {
        conf = new Configuration();
        job = Job.getInstance(conf);
        fakeTaskAttemptContext = mock(TaskAttemptContext.class);
        outputFormat = new PinotOutputFormat();
        outputTempDir = Files.createTempDirectory(PinotOutputFormatTest.class.getName() + "_io_output").toFile();
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
    public void verifyStarIndex() throws Exception {
        int days = 2000;
        int sal = 20;
        PinotOutputFormat.setEnableStarTreeIndex(job, true);
        RecordWriter<Object, byte[]> writer = outputFormat.getRecordWriter(fakeTaskAttemptContext);
        ObjectMapper mapper = new ObjectMapper();
        Map<Integer, Emp> inputMap = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            String name = "name " + i;
            Emp e = new Emp(i, name, days + i, sal + i);
            byte[] b = mapper.writeValueAsBytes(e);
            writer.write(null, b);
            inputMap.put(i, e);
        }
        writer.close(fakeTaskAttemptContext);

        String segmentTarPath = "_temporary/0/_temporary/attempt_foo_task_0123_r_000002_2/part-r-00002/segmentTar";
        File segmentTarOutput = new File(outputTempDir, segmentTarPath);
        File untarOutput = Files.createTempDirectory(PinotOutputFormatTest.class.getName() + "_segmentUnTar").toFile();
        for (File tarFile : segmentTarOutput.listFiles()) {
            TarGzCompressionUtils.unTar(tarFile, untarOutput);
        }

        File outputDir = new File(untarOutput, PinotOutputFormat.getSegmentName(fakeTaskAttemptContext));
        RecordReader recordReader = new PinotSegmentRecordReader(outputDir, Schema.fromString(getSchema()));
        GenericRow row = new GenericRow();
        Map<Integer, GenericRow> resultMap = new HashMap<>();
        while (recordReader.hasNext()) {
            row = recordReader.next(row);
            resultMap.put((Integer) row.getValue("id"), row);
            row.clear();
        }

        Assert.assertEquals(resultMap.size(), inputMap.size());
        // more data validation.
    }

    @Test
    public void verifyRawIndex() throws Exception {
        int days = 2000;
        int sal = 20;
        PinotOutputFormat.setEnableStarTreeIndex(job, false);
        RecordWriter<Object, byte[]> writer = outputFormat.getRecordWriter(fakeTaskAttemptContext);
        ObjectMapper mapper = new ObjectMapper();
        Map<Integer, Emp> inputMap = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            String name = "name " + i;
            Emp e = new Emp(i, name, days + i, sal + i);
            byte[] b = mapper.writeValueAsBytes(e);
            writer.write(null, b);
            inputMap.put(i, e);
        }
        writer.close(fakeTaskAttemptContext);

        String segmentTarPath = "_temporary/0/_temporary/attempt_foo_task_0123_r_000002_2/part-r-00002/segmentTar";
        File segmentTarOutput = new File(outputTempDir, segmentTarPath);
        File untarOutput = Files.createTempDirectory(PinotOutputFormatTest.class.getName() + "_segmentUnTar").toFile();
        for (File tarFile : segmentTarOutput.listFiles()) {
            TarGzCompressionUtils.unTar(tarFile, untarOutput);
        }

        File outputDir = new File(untarOutput, PinotOutputFormat.getSegmentName(fakeTaskAttemptContext));
        RecordReader recordReader = new PinotSegmentRecordReader(outputDir, Schema.fromString(getSchema()));
        GenericRow row = new GenericRow();
        Map<Integer, GenericRow> resultMap = new HashMap<>();
        while (recordReader.hasNext()) {
            row = recordReader.next(row);
            System.out.println(row);
            resultMap.put((Integer) row.getValue("id"), row);
            row.clear();
        }

        Assert.assertEquals(resultMap.size(), inputMap.size());
        // more data validation.
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
