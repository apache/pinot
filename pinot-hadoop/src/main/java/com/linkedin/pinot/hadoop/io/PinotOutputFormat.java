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
import com.linkedin.pinot.common.data.StarTreeIndexSpec;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.startree.hll.HllConfig;
import com.linkedin.pinot.startree.hll.HllConstants;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

/**
 * Generic Pinot Output Format implementation.
 * @param <K>
 * @param <V>
 */
public class PinotOutputFormat<K, V> extends FileOutputFormat<K, V> {


    private final SegmentGeneratorConfig _segmentConfig;

    // Pinot temp directory to create segment.
    public static final String TEMP_SEGMENT_DIR = "pinot.temp.segment.dir";

    // Name of the table
    public static final String TABLE_NAME = "pinot.table.name";

    // Name of the segment.
    public static final String SEGMENT_NAME = "pinot.segment_name";

    // file containing schema for the data
    public static final String SCHEMA = "pinot.schema.file";

    // config file for the record reader
    public static final String READER_CONFIG = "pinot.reader.config.file";

    // boolean flag to enable Star Tree Index.
    public static final String ENABLE_STAR_TREE_INDEX = "pinot.enable.star.tree.index";

    // Config file for star tree index.
    public static final String STAR_TREE_INDEX_SPEC = "pinot.star.tree.index.spec.file";

    // HLL size (log scale), default is 9.
    public static final String HLL_SIZE = "pinot.hll.size";

    // HLL columns
    public static final String HLL_COLUMNS = "pinot.hll.columns";

    // Suffix for the derived HLL columns
    public static final String HLL_SUFFIX = "pinot.hll.suffix";

    public static final String PINOT_RECORD_SERIALIZATION_CLASS = "pinot.record.serialization.class";


    public PinotOutputFormat() {
        _segmentConfig = new SegmentGeneratorConfig();
    }

    public static void setTempSegmentDir(Job job, String segmentDir) {
        job.getConfiguration().set(PinotOutputFormat.TEMP_SEGMENT_DIR, segmentDir);
    }

    public static String getTempSegmentDir(JobContext job) {
        return job.getConfiguration().get(PinotOutputFormat.TEMP_SEGMENT_DIR, ".data_" + getTableName(job));
    }

    public static void setTableName(Job job, String table) {
        job.getConfiguration().set(PinotOutputFormat.TABLE_NAME, table);
    }

    public static String getTableName(JobContext job) {
        String table = job.getConfiguration().get(PinotOutputFormat.TABLE_NAME);
        if (table == null) {
            throw new RuntimeException("pinot table name not set.");
        }
        return table;
    }

    public static void setSegmentName(Job job, String segmentName) {
        job.getConfiguration().set(PinotOutputFormat.SEGMENT_NAME, segmentName);
    }

    public static String getSegmentName(JobContext context) {
        String segment = context.getConfiguration().get(PinotOutputFormat.SEGMENT_NAME);
        if (segment == null) {
            throw new RuntimeException("pinot segment name not set.");
        }
        return segment;
    }

    public static void setSchema(Job job, Schema schema) {
        job.getConfiguration().set(PinotOutputFormat.SCHEMA, schema.getJSONSchema());
    }

    public static String getSchema(JobContext context) {
        String schemaFile = context.getConfiguration().get(PinotOutputFormat.SCHEMA);
        if (schemaFile == null) {
            throw new RuntimeException("pinot schema file not set");
        }
        return schemaFile;
    }

    public static void setReaderConfig(Job job, String readConfig) {
        job.getConfiguration().set(PinotOutputFormat.READER_CONFIG, readConfig);
    }

    public static String getReaderConfig(JobContext context) {
        return context.getConfiguration().get(PinotOutputFormat.READER_CONFIG);
    }

    public static void setEnableStarTreeIndex(Job job, boolean flag) {
        job.getConfiguration().setBoolean(PinotOutputFormat.ENABLE_STAR_TREE_INDEX, flag);
    }

    public static boolean getEnableStarTreeIndex(JobContext context) {
        return context.getConfiguration().getBoolean(PinotOutputFormat.ENABLE_STAR_TREE_INDEX, false);
    }

    public static void setStarTreeIndexSpec(Job job, String starTreeIndexSpec) {
        job.getConfiguration().set(PinotOutputFormat.STAR_TREE_INDEX_SPEC, starTreeIndexSpec);
    }

    public static String getStarTreeIndexSpec(JobContext context) {
        return context.getConfiguration().get(PinotOutputFormat.STAR_TREE_INDEX_SPEC);
    }

    public static void getHllSize(Job job, int size) {
        job.getConfiguration().setInt(PinotOutputFormat.HLL_SIZE, size);
    }

    public static int getHllSize(JobContext context) {
        return context.getConfiguration().getInt(PinotOutputFormat.HLL_SIZE, 9);
    }

    public static void setHllColumns(Job job, String columns) {
        job.getConfiguration().set(PinotOutputFormat.HLL_COLUMNS, columns);
    }

    public static String getHllColumns(JobContext context) {
        return context.getConfiguration().get(PinotOutputFormat.HLL_COLUMNS);
    }

    public static void getHllSuffix(Job job, String suffix) {
        job.getConfiguration().set(PinotOutputFormat.HLL_SUFFIX, suffix);
    }

    public static String getHllSuffix(JobContext context) {
        return context.getConfiguration().get(PinotOutputFormat.HLL_SUFFIX, HllConstants.DEFAULT_HLL_DERIVE_COLUMN_SUFFIX);
    }

    public static void setDataWriteSupportClass(Job job, Class<? extends PinotRecordSerialization> pinotSerialization) {
        job.getConfiguration().set(PinotOutputFormat.PINOT_RECORD_SERIALIZATION_CLASS, pinotSerialization.getName());
    }

    public static Class<?> getDataWriteSupportClass(JobContext context) {
        String className = context.getConfiguration().get(PinotOutputFormat.PINOT_RECORD_SERIALIZATION_CLASS);
        if (className == null) {
            throw new RuntimeException("pinot data write support class not set");
        }
        try {
            return context.getConfiguration().getClassByName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        configure(context.getConfiguration());
        final PinotRecordSerialization dataWriteSupport = getDataWriteSupport(context);
        initSegmentConfig(context);
        Path workDir = getDefaultWorkFile(context, "");
        return new PinotRecordWriter<>(_segmentConfig, context, workDir, dataWriteSupport);
    }

    /**
     * The {@link #configure(Configuration)} method called before initialize the  {@link
     * RecordWriter} Any implementation of {@link PinotOutputFormat} can use it to set additional
     * configuration properties.
     */
    public void configure(Configuration conf) {

    }

    private PinotRecordSerialization getDataWriteSupport(TaskAttemptContext context) {
        try {
            return (PinotRecordSerialization) PinotOutputFormat.getDataWriteSupportClass(context).newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Error initialize data write support class", e);
        }
    }

    private void initSegmentConfig(JobContext context) throws IOException {
        _segmentConfig.setFormat(FileFormat.JSON);
        _segmentConfig.setOutDir(PinotOutputFormat.getTempSegmentDir(context) + "/segmentDir");
        _segmentConfig.setOverwrite(true);
        _segmentConfig.setTableName(PinotOutputFormat.getTableName(context));
        _segmentConfig.setSegmentName(PinotOutputFormat.getSegmentName(context));
        _segmentConfig.setSchema(Schema.fromString(PinotOutputFormat.getSchema(context)));
        _segmentConfig.setReaderConfigFile(PinotOutputFormat.getReaderConfig(context));
        initStarTreeIndex(context);
        initHllConfig(context);
    }

    private void initHllConfig(JobContext context) {
        String _hllColumns = PinotOutputFormat.getHllColumns(context);
        if (_hllColumns != null) {
            String[] hllColumns = StringUtils.split(StringUtils.deleteWhitespace(_hllColumns), ',');
            if (hllColumns.length != 0) {
                HllConfig hllConfig = new HllConfig(PinotOutputFormat.getHllSize(context));
                hllConfig.setColumnsToDeriveHllFields(new HashSet<>(Arrays.asList(hllColumns)));
                hllConfig.setHllDeriveColumnSuffix(PinotOutputFormat.getHllSuffix(context));
                _segmentConfig.setHllConfig(hllConfig);
            }
        }
    }

    private void initStarTreeIndex(JobContext context) throws IOException {
        String _starTreeIndexSpecFile = PinotOutputFormat.getStarTreeIndexSpec(context);
        if (_starTreeIndexSpecFile != null) {
            StarTreeIndexSpec starTreeIndexSpec = StarTreeIndexSpec.fromFile(new File(_starTreeIndexSpecFile));
            // Specifying star-tree index file enables star tree generation, even if _enableStarTreeIndex is not specified.
            _segmentConfig.enableStarTreeIndex(starTreeIndexSpec);
        } else if (PinotOutputFormat.getEnableStarTreeIndex(context)) {
            _segmentConfig.enableStarTreeIndex(null);
        }
    }
}
