/*
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

import com.linkedin.pinot.tools.Command;
import com.linkedin.pinot.tools.admin.SchemaInfo;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import static com.linkedin.pinot.tools.admin.SchemaInfo.*;

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

    private void startZookeeper() throws IOException {
        StartZookeeperCommand zkStarter = new StartZookeeperCommand();
        zkStarter.execute();
    }

    private void startContollers() throws Exception {
        StartControllerCommand controllerStarter = new StartControllerCommand();
        controllerStarter.setControllerPort(SchemaInfo.DEFAULT_CONTROLLER_PORT)
            .setZkAddress(DEFAULT_ZOOKEEPER_ADDRESS)
            .setDataDir(DEFAULT_DATA_DIR);
        controllerStarter.execute();
    }

    private void startBrokers() throws Exception {
        StartBrokerCommand brokerStarter = new StartBrokerCommand();
        brokerStarter.setPort(DEFAULT_BROKER_PORT)
                .setZkAddress(DEFAULT_ZOOKEEPER_ADDRESS);
        brokerStarter.execute();
    }

    private void startServers() throws Exception {
        StartServerCommand serverStarter = new StartServerCommand();
        serverStarter.setPort(DEFAULT_SERVER_PORT)
                .setDataDir(DATA_DIR)
                .setSegmentDir(SEGMENT_DIR)
                .setZkAddress(DEFAULT_ZOOKEEPER_ADDRESS);
        serverStarter.execute();
    }

    private void startAllServices() throws Exception {
        startZookeeper();
        startContollers();
        startBrokers();
        startServers();
    }

    public void stopAllServices() throws Exception {
        StopProcessCommand stopper = new StopProcessCommand(false);
        stopper.stopController().stopBroker().stopServer().stopZookeeper();
        stopper.execute();
    }

    private void generateData(int numRecords, int numFiles, String schemaFile, String schemaAnnotationFile,
                              String outputDataDir) throws Exception {
        ClassLoader classLoader = SegmentCreationCommand.class.getClassLoader();
        URL resource = classLoader.getResource(schemaFile);

        GenerateDataCommand generator = new GenerateDataCommand();
        generator.init(numRecords, numFiles, resource.toString().substring(5), outputDataDir);

        resource = classLoader.getResource(schemaAnnotationFile);
        generator.set_schemaAnnFile(resource.toString().substring(5));

        generator.execute();
    }

    private void createSegments(String schemaFile, String dataDir, String segmentName, String tableName,
                                String segOutputDir) throws Exception {
        ClassLoader classLoader = SegmentCreationCommand.class.getClassLoader();
        URL resource = classLoader.getResource(schemaFile);

        CreateSegmentCommand segmentCreator = new CreateSegmentCommand();
        segmentCreator.setDataDir(dataDir)
                .setSchemaFile(resource.toString().substring(5))
                .setSegmentName(segmentName)
                .setTableName(tableName)
                .setOutDir(segOutputDir);
        segmentCreator.execute();

    }

    private void addTable(String tableSchema) throws Exception {
        ClassLoader classLoader = SegmentCreationCommand.class.getClassLoader();
        URL resource = classLoader.getResource(tableSchema);

        AddTableCommand adder = new AddTableCommand();
        adder.setFilePath(resource.toString().substring(5))
                .setControllerPort(SchemaInfo.DEFAULT_CONTROLLER_PORT)
                .setExecute(true);
        adder.execute();
    }


    @Override
    public boolean execute() throws Exception {
        SegmentCreationCommand seg = new SegmentCreationCommand();

        String PARENT_FOLDER = _dir + "/";
        String DATA_DIR = "data" + "/";
        String SEG_DIR = "segment" + "/";

        LOGGER.info("----- Starting All Services -----");
        seg.startAllServices();

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
                    SEGMENT_NAME, TABLE_NAMES.get(i), PARENT_FOLDER + SEG_DIR + SchemaInfo.DATA_DIRS.get(i));

            LOGGER.info("----- Adding Table Schema -----");
            seg.addTable(TABLE_DEFINITIONS.get(i));

        }

        return true;
    }

}
