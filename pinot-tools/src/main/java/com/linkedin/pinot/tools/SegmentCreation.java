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
package com.linkedin.pinot.tools;
import com.linkedin.pinot.tools.admin.command.*;
import static com.linkedin.pinot.tools.SchemaInfo.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.io.File;

public class SegmentCreation {
    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentCreation.class);

    SegmentCreation() { }

    private void startZookeeper() throws IOException {
        StartZookeeperCommand zkStarter = new StartZookeeperCommand();
        zkStarter.execute();
    }

    private void startContollers() throws Exception {
        StartControllerCommand controllerStarter = new StartControllerCommand();
        controllerStarter.setControllerPort(DEFAULT_CONTROLLER_PORT)
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

    public void startAllServices() throws Exception {
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

    public void generateData(int numRecords, int numFiles, String schemaFile, String schemaAnnotationFile,
                             String outputDataDir) throws Exception {
        ClassLoader classLoader = SegmentCreation.class.getClassLoader();
        URL resource = classLoader.getResource(schemaFile);

        GenerateDataCommand generator = new GenerateDataCommand();
        generator.init(numRecords, numFiles, resource.toString().substring(5), outputDataDir);

        resource = classLoader.getResource(schemaAnnotationFile);
        generator.set_schemaAnnFile(resource.toString().substring(5));

        generator.execute();
    }

    public void createSegments(String schemaFile, String dataDir, String segmentName, String tableName,
                               String segOutputDir) throws Exception {
        ClassLoader classLoader = SegmentCreation.class.getClassLoader();
        URL resource = classLoader.getResource(schemaFile);
        System.out.println(resource.toString().substring(5));

        CreateSegmentCommand segmentCreator = new CreateSegmentCommand();
        segmentCreator.setDataDir(dataDir)
                .setSchemaFile(resource.toString().substring(5))
                .setSegmentName(segmentName)
                .setTableName(tableName)
                .setOutDir(segOutputDir);
        segmentCreator.execute();

    }

    public void addTable(String tableSchema) throws Exception {
        ClassLoader classLoader = SegmentCreation.class.getClassLoader();
        URL resource = classLoader.getResource(tableSchema);
        System.out.println(resource.toString().substring(5));

        AddTableCommand adder = new AddTableCommand();
        adder.setFilePath(resource.toString().substring(5))
                .setControllerPort(DEFAULT_CONTROLLER_PORT)
                .setExecute(true);
        adder.execute();
    }

    public void uploadSegments(String segDir) throws Exception {
        UploadSegmentCommand uploader = new UploadSegmentCommand();
        uploader.setControllerPort(DEFAULT_CONTROLLER_PORT)
                .setSegmentDir(segDir);
        uploader.execute();
    }

    public static void main(String[] args) throws Exception {
        SegmentCreation seg = new SegmentCreation();

        LOGGER.info("----- Starting All Services -----");
        seg.startAllServices();

        for (int i = 0; i < SCHEMAS.size(); i++) {
            LOGGER.info("----- Generating data -----");
            File file = new File("data");
            if (!file.exists())
                file.mkdir();
            seg.generateData(NUM_RECORDS.get(i), NUM_SEGMENTS.get(i), SCHEMAS.get(i), SCHEMAS_ANNOTATIONS.get(i),
                    "data/" + SchemaInfo.DATA_DIRS.get(i));

            LOGGER.info("----- Creating Segments -----");
            file = new File("segment");
            if (!file.exists())
                file.mkdir();
            seg.createSegments(SCHEMAS.get(i), "data/" + SchemaInfo.DATA_DIRS.get(i), SEGMENT_NAME, TABLE_NAMES.get(i),
                    "segment/" + SchemaInfo.DATA_DIRS.get(i));

            LOGGER.info("----- Adding Table Schema -----");
            seg.addTable(TABLE_DEFINITIONS.get(i));

        }

    }
}
