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
package com.linkedin.pinot.tools.admin.command;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.tools.Command;
import com.linkedin.pinot.tools.SchemaInfo;
import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Timestamp;

import static com.linkedin.pinot.tools.SchemaInfo.*;

public class SegmentCreationCommand extends AbstractBaseAdminCommand implements Command {
    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentCreationCommand.class);

    @Option(name="-help", required=false, help=true, aliases={"-h", "--h", "--help"}, usage="Print this message.")
    private boolean _help = false;

    @Option(name = "-dir", required = true, metaVar = "<String>", usage = "Parent directory to store data & segments.")
    private String _dir;

    @Override
    public String description() {
        return "Create data and segments as per the provided schema";
    }

    @Override
    public boolean getHelp() {
        return _help;
    }

    private void generateData(int numRecords, int numFiles, String schemaFile, String schemaAnnotationFile,
                              String outputDataDir) throws Exception {

        final File tmpDir = new File("temp");

        if (!tmpDir.exists()) {
            Preconditions.checkState(tmpDir.mkdirs());
        }

        File file = new File(tmpDir, "tempSchemaFile.json");
        ClassLoader classLoader = SegmentCreationCommand.class.getClassLoader();
        URL resource = classLoader.getResource(schemaFile);
        com.google.common.base.Preconditions.checkNotNull(resource);
        FileUtils.copyURLToFile(resource, file);
        GenerateDataCommand generator = new GenerateDataCommand();
        generator.init(numRecords, numFiles, file.getAbsolutePath(), outputDataDir);

        File ann_file = new File(tmpDir, "tempSchemaAnnFile.json");
        resource = classLoader.getResource(schemaAnnotationFile);
        com.google.common.base.Preconditions.checkNotNull(resource);
        FileUtils.copyURLToFile(resource, ann_file);
        generator.set_schemaAnnFile(ann_file.getAbsolutePath());

        generator.execute();
    }

    private void createSegments(String schemaFile, String dataDir, String segmentName, String tableName,
                                String segOutputDir) throws Exception {

        final File tmpDir = new File("temp");
        File file = new File(tmpDir, "tempSchemaFile.json");
        ClassLoader classLoader = SegmentCreationCommand.class.getClassLoader();
        URL resource = classLoader.getResource(schemaFile);
        com.google.common.base.Preconditions.checkNotNull(resource);
        FileUtils.copyURLToFile(resource, file);

        CreateSegmentCommand segmentCreator = new CreateSegmentCommand();
        segmentCreator.setDataDir(dataDir)
                .setSchemaFile(file.getAbsolutePath())
                .setSegmentName(segmentName)
                .setTableName(tableName)
                .setOutDir(segOutputDir);

        segmentCreator.execute();
    }


    @Override
    public boolean execute() throws Exception {
        SegmentCreationCommand seg = new SegmentCreationCommand();

        String PARENT_FOLDER = _dir + "/";
        String DATA_DIR = "data" + "/";
        String SEG_DIR = "segment" + "/";

        for (int i = 0; i < SCHEMAS.size(); i++) {
            LOGGER.info("----- Generating data -----");
            File file = new File(PARENT_FOLDER + DATA_DIR);
            if (!file.exists())
                file.mkdirs();
            seg.generateData(NUM_RECORDS.get(i), NUM_SEGMENTS.get(i), SCHEMAS.get(i), SCHEMAS_ANNOTATIONS.get(i),
                    PARENT_FOLDER + DATA_DIR + SchemaInfo.DATA_DIRS.get(i));

            LOGGER.info("----- Creating Segments -----");
            file = new File(PARENT_FOLDER + SEG_DIR);
            if (!file.exists())
                file.mkdirs();
            seg.createSegments(SCHEMAS.get(i), PARENT_FOLDER + DATA_DIR + SchemaInfo.DATA_DIRS.get(i),
                    SEGMENT_NAME + "_" + new Timestamp(System.currentTimeMillis()), TABLE_NAMES.get(i),
                    PARENT_FOLDER + SEG_DIR + SchemaInfo.DATA_DIRS.get(i));

            /* Deleting data directory */
            FileUtils.deleteDirectory(new File(PARENT_FOLDER + DATA_DIR));

        }

        /* Deleting temporary directory used to store config files */
        FileUtils.deleteDirectory(new File("temp"));

        return true;
    }

}
