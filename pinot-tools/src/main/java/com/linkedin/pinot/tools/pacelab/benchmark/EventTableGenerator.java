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

package com.linkedin.pinot.tools.pacelab.benchmark;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.controller.helix.core.sharding.LatencyBasedLoadMetric;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.extractors.FieldExtractor;
import com.linkedin.pinot.core.data.extractors.FieldExtractorFactory;
import com.linkedin.pinot.core.data.readers.AvroRecordReader;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.tools.data.generator.RangeIntGenerator;
import com.linkedin.pinot.tools.data.generator.RangeLongGenerator;
import com.linkedin.pinot.tools.data.generator.SchemaAnnotation;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import java.net.URL;

public class EventTableGenerator {
    private String _dataDir;
    private String _outDir;
    private boolean _overwrite = true;
    private int _numRecords = 10000;
    private long _timeColumnStart;
    private long _timeColumnEnd;

    final String _profileSchemaFile = "pinot_benchmark/main_schemas/ProfileSchema.json";
    final String _profileViewSchemaFile = "pinot_benchmark/main_schemas/ProfileViewSchema.json";
    final String _profileViewSchemaAnnFile = "pinot_benchmark/main_schemas/ProfileViewSchemaAnnotation.json";

    final String _adSchemaFile = "pinot_benchmark/main_schemas/AdSchema.json";
    final String _adClickSchemaFile = "pinot_benchmark/main_schemas/AdClickSchema.json";
    final String _adClickSchemaAnnFile = "pinot_benchmark/main_schemas/AdClickSchemaAnnotation.json";

    final String _jobSchemaFile = "pinot_benchmark/main_schemas/JobSchema.json";
    final String _jobApplySchemaFile = "pinot_benchmark/main_schemas/JobApplySchema.json";
    final String _jobApplySchemaAnnFile = "pinot_benchmark/main_schemas/JobApplySchemaAnnotation.json";

    final String _articleSchemaFile = "pinot_benchmark/main_schemas/ArticleSchema.json";
    final String _articleReadSchemaFile = "pinot_benchmark/main_schemas/ArticleReadSchema.json";
    final String _articleReadSchemaAnnFile = "pinot_benchmark/main_schemas/ArticleReadSchemaAnnotation.json";

    public EventTableGenerator(String dataDir)
    {
        _dataDir = dataDir;
    }
    public EventTableGenerator(String dataDir, String outDir, int numRecords)
    {
        _dataDir = dataDir;
        _outDir = outDir;
        _numRecords = numRecords;
    }

    public  List<GenericRow> readProfileTable() throws Exception
    {
        String profileDataFile = getTableDataDirectory("profile");
        ClassLoader classLoader = LatencyBasedLoadMetric.class.getClassLoader();
        String profileSchemaFile = getFileFromResourceUrl(classLoader.getResource(_profileSchemaFile));
        List<GenericRow> profileTable = readBaseTableData(profileSchemaFile,profileDataFile);
        return profileTable;
    }
    public  List<GenericRow> readJobTable() throws Exception
    {
        String jobDataFile = getTableDataDirectory("job");
        ClassLoader classLoader = LatencyBasedLoadMetric.class.getClassLoader();
        String jobSchemaFile = getFileFromResourceUrl(classLoader.getResource(_jobSchemaFile));
        List<GenericRow> jobTable = readBaseTableData(jobSchemaFile,jobDataFile);
        return jobTable;
    }

    public  List<GenericRow> readAdTable() throws Exception
    {
        String adDataFile = getTableDataDirectory("ad");
        ClassLoader classLoader = LatencyBasedLoadMetric.class.getClassLoader();
        String adSchemaFile = getFileFromResourceUrl(classLoader.getResource(_adSchemaFile));
        List<GenericRow> adTable = readBaseTableData(adSchemaFile,adDataFile);
        return adTable;
    }

    public  List<GenericRow> readArticleTable() throws Exception
    {
        String articleDataFile = getTableDataDirectory("article");
        ClassLoader classLoader = LatencyBasedLoadMetric.class.getClassLoader();
        String articleSchemaFile = getFileFromResourceUrl(classLoader.getResource(_articleReadSchemaFile));
        List<GenericRow> articleTable = readBaseTableData(articleSchemaFile,articleDataFile);
        return articleTable;
    }

    public String getTableDataDirectory(String tableName)
    {
        String  tableDatDir;
        if(_dataDir.endsWith("/"))
        {
            tableDatDir = _dataDir + tableName;
        }
        else
        {
            tableDatDir = _dataDir + "/" + tableName;
        }

        // Filter out all input files.
        File dir = new File(tableDatDir);
        if (!dir.exists() || !dir.isDirectory()) {
            throw new RuntimeException("Data directory " + tableDatDir + " not found.");
        }

        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith("avro");
            }
        });

        if ((files == null) || (files.length == 0)) {
            throw new RuntimeException(
                    "Data directory " + _dataDir + " does not contain " + "avro" + " files.");
        }

        return  files[0].getAbsolutePath();
    }

    public List<GenericRow> readBaseTableData(String schemaFile, String dataFile) throws Exception
    {
         List<GenericRow> tableData = new ArrayList<GenericRow>();

        Schema profileSchema = Schema.fromFile(new File(schemaFile));
        FieldExtractor extractor = FieldExtractorFactory.getPlainFieldExtractor(profileSchema);
        RecordReader reader =  new AvroRecordReader(extractor, dataFile);
        reader.init();
        while(reader.hasNext())
        {
            tableData.add(reader.next());
        }
        return tableData;
    }

    private List<SchemaAnnotation> readSchemaAnnotationFile(String schemaAnnotationFile) throws Exception
    {
        ObjectMapper objectMapper = new ObjectMapper();
        List<SchemaAnnotation> saList = objectMapper.readValue(new File(schemaAnnotationFile), new TypeReference<List<SchemaAnnotation>>() {});
        return saList;
    }

    private RangeLongGenerator createLongRangeGenerator(List<SchemaAnnotation> saList, String columnName)
    {
        long end = System.currentTimeMillis()/1000;
        long start = end - (30*24*3600);

        for (SchemaAnnotation sa : saList) {
            String column = sa.getColumn();
            if(column.equalsIgnoreCase(columnName))
            {
                start = sa.getRangeStart();
                end = sa.getRangeEnd();
            }
        }
        return  new RangeLongGenerator(start, end);
    }

    private RangeIntGenerator createIntRangeGenerator(List<SchemaAnnotation> saList, String columnName)
    {
        int end = 1800;
        int start = 1;

        for (SchemaAnnotation sa : saList) {
            String column = sa.getColumn();
            if(column.equalsIgnoreCase(columnName))
            {
                start = sa.getRangeStart();
                end = sa.getRangeEnd();
            }
        }
        return  new RangeIntGenerator(start, end);
    }

    private File createOutDirAndFile(String outDirName) throws Exception
    {
        String outDirPath;
        if(_outDir.endsWith("/"))
        {
            outDirPath = _outDir + outDirName;
        }
        else
        {
            outDirPath = _outDir + "/" + outDirName;
        }
        // Make sure output directory does not already exist, or can be overwritten.
        File outDir = new File(outDirPath);
        if (outDir.exists()) {
            if (!_overwrite) {
                throw new IOException("Output directory " + outDirPath + " already exists.");
            } else {
                FileUtils.deleteDirectory(outDir);
            }
        }

        outDir.mkdir();


        return new File(outDirPath, "part-" + 0 + ".avro");
    }

    private DataFileWriter<GenericData.Record> createRecordWriter(String schemaFile, File avroFile) throws Exception
    {
        org.apache.avro.Schema schemaJSON = org.apache.avro.Schema.parse(getJSONSchema(Schema.fromFile(new File(schemaFile))).toString());
        final GenericDatumWriter<GenericData.Record> datum = new GenericDatumWriter<GenericData.Record>(schemaJSON);
        DataFileWriter<GenericData.Record> recordWriter = new DataFileWriter<GenericData.Record>(datum);
        recordWriter.create(schemaJSON, avroFile);
        return recordWriter;

    }

    public static String getFileFromResourceUrl(@Nonnull URL resourceUrl) {
        // For maven cross package use case, we need to extract the resource from jar to a temporary directory.
        String resourceUrlStr = resourceUrl.toString();
        if (resourceUrlStr.contains("jar!")) {
            try {
                String extension = resourceUrlStr.substring(resourceUrlStr.lastIndexOf('.'));
                File tempFile = File.createTempFile("pinot-test-temp", extension);
                String tempFilePath = tempFile.getAbsolutePath();
                //LOGGER.info("Extracting from " + resourceUrlStr + " to " + tempFilePath);
                FileUtils.copyURLToFile(resourceUrl, tempFile);
                return tempFilePath;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            return resourceUrl.getFile();
        }
    }


    public boolean generateProfileViewTable(long timeColumnStart, long timeColumnEnd, int numRecords) throws Exception
    {

        ClassLoader classLoader = EventTableGenerator.class.getClassLoader();
        String profileSchemaFile = getFileFromResourceUrl(classLoader.getResource(_profileSchemaFile));
        String profileViewSchemaFile = getFileFromResourceUrl(classLoader.getResource(_profileViewSchemaFile));
        String profileViewSchemaAnn = getFileFromResourceUrl(classLoader.getResource(_profileViewSchemaAnnFile));

        String profileDataFile = getTableDataDirectory("profile");
        List<GenericRow> profileTable = readBaseTableData(profileSchemaFile,profileDataFile);
        List<SchemaAnnotation> saList = readSchemaAnnotationFile(profileViewSchemaAnn);
        RangeLongGenerator eventTimeGenerator = new RangeLongGenerator(timeColumnStart, timeColumnEnd);
        RangeIntGenerator timeSpentGenerator = createIntRangeGenerator(saList,"ReviewTime");
        File avroFile = createOutDirAndFile("ProfileView");
        DataFileWriter<GenericData.Record> recordWriter = createRecordWriter(profileViewSchemaFile,avroFile);

        org.apache.avro.Schema schemaJSON = org.apache.avro.Schema.parse(getJSONSchema(Schema.fromFile(new File(profileViewSchemaFile))).toString());

        for(int i=0;i<numRecords;i++)
        {
            final GenericData.Record outRecord = new GenericData.Record(schemaJSON);
            GenericRow viewerProfile = getRandomGenericRow(profileTable);
            GenericRow viewedProfile = getRandomGenericRow(profileTable);
            while(viewedProfile == viewerProfile)
            {
                viewedProfile = getRandomGenericRow(profileTable);
            }

            outRecord.put("ViewStartTime", eventTimeGenerator.next());
            outRecord.put("ReviewTime",timeSpentGenerator.next());
            outRecord.put("ViewerProfileStrength",  viewerProfile.getValue("Strength"));
            outRecord.put("ViewedProfileStrength", viewedProfile.getValue("Strength"));
            outRecord.put("ViewerProfileId", viewerProfile.getValue("ID"));
            outRecord.put("ViewerWorkPlace", viewerProfile.getValue("WorkPlace"));
            outRecord.put("ViewerHeadline", viewerProfile.getValue("Headline"));
            outRecord.put("ViewerPosition", viewerProfile.getValue("Position"));
            outRecord.put("ViewedProfileId", viewedProfile.getValue("ID"));
            outRecord.put("ViewedProfileWorkPlace", viewerProfile.getValue("ViewedProfileWorkPlace"));
            outRecord.put("ViewedProfileHeadline", viewedProfile.getValue("Headline"));
            outRecord.put("ViewedProfilePosition", viewedProfile.getValue("Position"));
            outRecord.put("WereProfilesConnected", randomYesOrNo());

            recordWriter.append(outRecord);
        }

        recordWriter.close();
        return true;
    }

    public boolean generateProfileViewTable() throws Exception
    {

        ClassLoader classLoader = EventTableGenerator.class.getClassLoader();
        String profileSchemaFile = getFileFromResourceUrl(classLoader.getResource(_profileSchemaFile));
        String profileViewSchemaFile = getFileFromResourceUrl(classLoader.getResource(_profileViewSchemaFile));
        String profileViewSchemaAnn = getFileFromResourceUrl(classLoader.getResource(_profileViewSchemaAnnFile));

        String profileDataFile = getTableDataDirectory("profile");
        List<GenericRow> profileTable = readBaseTableData(profileSchemaFile,profileDataFile);
        List<SchemaAnnotation> saList = readSchemaAnnotationFile(profileViewSchemaAnn);
        RangeLongGenerator eventTimeGenerator = createLongRangeGenerator(saList,"ViewStartTime");
        RangeIntGenerator timeSpentGenerator = createIntRangeGenerator(saList,"ReviewTime");
        File avroFile = createOutDirAndFile("ProfileView");
        DataFileWriter<GenericData.Record> recordWriter = createRecordWriter(profileViewSchemaFile,avroFile);

        org.apache.avro.Schema schemaJSON = org.apache.avro.Schema.parse(getJSONSchema(Schema.fromFile(new File(profileViewSchemaFile))).toString());

        for(int i=0;i<_numRecords;i++)
        {
            final GenericData.Record outRecord = new GenericData.Record(schemaJSON);
            GenericRow viewerProfile = getRandomGenericRow(profileTable);
            GenericRow viewedProfile = getRandomGenericRow(profileTable);
            while(viewedProfile == viewerProfile)
            {
                viewedProfile = getRandomGenericRow(profileTable);
            }

            outRecord.put("ViewStartTime", eventTimeGenerator.next());
            outRecord.put("ReviewTime",timeSpentGenerator.next());
            outRecord.put("ViewerProfileStrength",  viewerProfile.getValue("Strength"));
            outRecord.put("ViewedProfileStrength", viewedProfile.getValue("Strength"));
            outRecord.put("ViewerProfileId", viewerProfile.getValue("ID"));
            outRecord.put("ViewerWorkPlace", viewerProfile.getValue("WorkPlace"));
            outRecord.put("ViewerHeadline", viewerProfile.getValue("Headline"));
            outRecord.put("ViewerPosition", viewerProfile.getValue("Position"));
            outRecord.put("ViewedProfileId", viewedProfile.getValue("ID"));
            outRecord.put("ViewedProfileWorkPlace", viewerProfile.getValue("ViewedProfileWorkPlace"));
            outRecord.put("ViewedProfileHeadline", viewedProfile.getValue("Headline"));
            outRecord.put("ViewedProfilePosition", viewedProfile.getValue("Position"));
            outRecord.put("WereProfilesConnected", randomYesOrNo());

            recordWriter.append(outRecord);
        }

        recordWriter.close();
        return true;
    }

    public boolean generateAdClickTable(long timeColumnStart, long timeColumnEnd, int numRecords) throws Exception
    {
        ClassLoader classLoader = EventTableGenerator.class.getClassLoader();
        String profileSchemaFile = getFileFromResourceUrl(classLoader.getResource(_profileSchemaFile));
        String adSchemaFile = getFileFromResourceUrl(classLoader.getResource(_adSchemaFile));
        String adClickSchemaAnn = getFileFromResourceUrl(classLoader.getResource(_adClickSchemaAnnFile));
        String adClickSchemaFile = getFileFromResourceUrl(classLoader.getResource(_adClickSchemaFile));

        String profileDataFile = getTableDataDirectory("profile");
        List<GenericRow> profileTable = readBaseTableData(profileSchemaFile,profileDataFile);

        String adDataFile = getTableDataDirectory("ad");
        List<GenericRow> adTable = readBaseTableData(adSchemaFile,adDataFile);

        List<SchemaAnnotation> saList = readSchemaAnnotationFile(adClickSchemaAnn);
        RangeLongGenerator eventTimeGenerator = new RangeLongGenerator(timeColumnStart, timeColumnEnd);;
        File avroFile = createOutDirAndFile("AdClick");
        DataFileWriter<GenericData.Record> recordWriter = createRecordWriter(adClickSchemaFile,avroFile);

        org.apache.avro.Schema schemaJSON = org.apache.avro.Schema.parse(getJSONSchema(Schema.fromFile(new File(adClickSchemaFile))).toString());

        for(int i=0;i<numRecords;i++)
        {
            final GenericData.Record outRecord = new GenericData.Record(schemaJSON);

            GenericRow viewerProfile = getRandomGenericRow(profileTable);
            GenericRow adInfo = getRandomGenericRow(adTable);

            outRecord.put("ClickTime", eventTimeGenerator.next());
            outRecord.put("ViewerStrength", viewerProfile.getValue("Strength"));
            outRecord.put("ViewerProfileId", viewerProfile.getValue("ID"));
            outRecord.put("ViewerHeadline", viewerProfile.getValue("Headline"));
            outRecord.put("ViewerPosition", viewerProfile.getValue("Position"));
            outRecord.put("AdID", adInfo.getValue("ID"));
            outRecord.put("AdUrl", adInfo.getValue("Url"));
            outRecord.put("AdTitle", adInfo.getValue("Title"));
            outRecord.put("AdText", adInfo.getValue("Text"));
            outRecord.put("AdCompany", adInfo.getValue("Company"));
            outRecord.put("AdCampaign", adInfo.getValue("Campaign"));

            recordWriter.append(outRecord);
        }

        recordWriter.close();
        return true;
    }

    public boolean generateAdClickTable() throws Exception
    {
        ClassLoader classLoader = EventTableGenerator.class.getClassLoader();
        String profileSchemaFile = getFileFromResourceUrl(classLoader.getResource(_profileSchemaFile));
        String adSchemaFile = getFileFromResourceUrl(classLoader.getResource(_adSchemaFile));
        String adClickSchemaAnn = getFileFromResourceUrl(classLoader.getResource(_adClickSchemaAnnFile));
        String adClickSchemaFile = getFileFromResourceUrl(classLoader.getResource(_adClickSchemaFile));

        String profileDataFile = getTableDataDirectory("profile");
        List<GenericRow> profileTable = readBaseTableData(profileSchemaFile,profileDataFile);

        String adDataFile = getTableDataDirectory("ad");
        List<GenericRow> adTable = readBaseTableData(adSchemaFile,adDataFile);

        List<SchemaAnnotation> saList = readSchemaAnnotationFile(adClickSchemaAnn);
        RangeLongGenerator eventTimeGenerator = createLongRangeGenerator(saList,"ClickTime");
        File avroFile = createOutDirAndFile("AdClick");
        DataFileWriter<GenericData.Record> recordWriter = createRecordWriter(adClickSchemaFile,avroFile);

        org.apache.avro.Schema schemaJSON = org.apache.avro.Schema.parse(getJSONSchema(Schema.fromFile(new File(adClickSchemaFile))).toString());

        for(int i=0;i<_numRecords;i++)
        {
            final GenericData.Record outRecord = new GenericData.Record(schemaJSON);

            GenericRow viewerProfile = getRandomGenericRow(profileTable);
            GenericRow adInfo = getRandomGenericRow(adTable);

            outRecord.put("ClickTime", eventTimeGenerator.next());
            outRecord.put("ViewerStrength", viewerProfile.getValue("Strength"));
            outRecord.put("ViewerProfileId", viewerProfile.getValue("ID"));
            outRecord.put("ViewerHeadline", viewerProfile.getValue("Headline"));
            outRecord.put("ViewerPosition", viewerProfile.getValue("Position"));
            outRecord.put("AdID", adInfo.getValue("ID"));
            outRecord.put("AdUrl", adInfo.getValue("Url"));
            outRecord.put("AdTitle", adInfo.getValue("Title"));
            outRecord.put("AdText", adInfo.getValue("Text"));
            outRecord.put("AdCompany", adInfo.getValue("Company"));
            outRecord.put("AdCampaign", adInfo.getValue("Campaign"));

            recordWriter.append(outRecord);
        }

        recordWriter.close();
        return true;
    }


    public boolean generateJobApplyTable(long timeColumnStart, long timeColumnEnd, int numRecords) throws Exception
    {
        ClassLoader classLoader = EventTableGenerator.class.getClassLoader();
        String profileSchemaFile = getFileFromResourceUrl(classLoader.getResource(_profileSchemaFile));
        String jobSchemaFile = getFileFromResourceUrl(classLoader.getResource(_jobSchemaFile));
        String jobApplySchemaAnn = getFileFromResourceUrl(classLoader.getResource(_jobApplySchemaAnnFile));
        String jobApplySchemaFile = getFileFromResourceUrl(classLoader.getResource(_jobApplySchemaFile));

        String profileDataFile = getTableDataDirectory("profile");
        List<GenericRow> profileTable = readBaseTableData(profileSchemaFile,profileDataFile);

        String jobDataFile = getTableDataDirectory("job");
        List<GenericRow> jobTable = readBaseTableData(jobSchemaFile,jobDataFile);

        List<SchemaAnnotation> saList = readSchemaAnnotationFile(jobApplySchemaAnn);
        RangeLongGenerator eventTimeGenerator = new RangeLongGenerator(timeColumnStart, timeColumnEnd);
        RangeIntGenerator timeSpentGenerator = createIntRangeGenerator(saList,"TimeSpent");

        File avroFile = createOutDirAndFile("JobApply");
        DataFileWriter<GenericData.Record> recordWriter = createRecordWriter(jobApplySchemaFile,avroFile);

        org.apache.avro.Schema schemaJSON = org.apache.avro.Schema.parse(getJSONSchema(Schema.fromFile(new File(jobApplySchemaFile))).toString());

        for(int i=0;i<numRecords;i++)
        {
            final GenericData.Record outRecord = new GenericData.Record(schemaJSON);

            GenericRow applicantProfile = getRandomGenericRow(profileTable);
            GenericRow jobInfo = getRandomGenericRow(jobTable);

            outRecord.put("ApplyStartTime", eventTimeGenerator.next());
            outRecord.put("TimeSpent", timeSpentGenerator.next());
            outRecord.put("JobSalary", jobInfo.getValue("Salary"));
            outRecord.put("ApplicantProfileId", applicantProfile.getValue("ID"));
            outRecord.put("ApplicantHeadline", applicantProfile.getValue("Headline"));
            outRecord.put("ApplicantPosition", applicantProfile.getValue("Position"));
            outRecord.put("JobID", jobInfo.getValue("ID"));
            outRecord.put("JobUrl", jobInfo.getValue("Url"));
            outRecord.put("JobTitle", jobInfo.getValue("Title"));
            outRecord.put("JobCompany", jobInfo.getValue("Company"));
            outRecord.put("DidApplyIsFinalized", randomYesOrNo());

            recordWriter.append(outRecord);
        }

        recordWriter.close();
        return true;
    }

    public boolean generateJobApplyTable() throws Exception
    {
        ClassLoader classLoader = EventTableGenerator.class.getClassLoader();
        String profileSchemaFile = getFileFromResourceUrl(classLoader.getResource(_profileSchemaFile));
        String jobSchemaFile = getFileFromResourceUrl(classLoader.getResource(_jobSchemaFile));
        String jobApplySchemaAnn = getFileFromResourceUrl(classLoader.getResource(_jobApplySchemaAnnFile));
        String jobApplySchemaFile = getFileFromResourceUrl(classLoader.getResource(_jobApplySchemaFile));

        String profileDataFile = getTableDataDirectory("profile");
        List<GenericRow> profileTable = readBaseTableData(profileSchemaFile,profileDataFile);

        String jobDataFile = getTableDataDirectory("job");
        List<GenericRow> jobTable = readBaseTableData(jobSchemaFile,jobDataFile);

        List<SchemaAnnotation> saList = readSchemaAnnotationFile(jobApplySchemaAnn);
        RangeLongGenerator eventTimeGenerator = createLongRangeGenerator(saList,"ApplyStartTime");
        RangeIntGenerator timeSpentGenerator = createIntRangeGenerator(saList,"TimeSpent");

        File avroFile = createOutDirAndFile("JobApply");
        DataFileWriter<GenericData.Record> recordWriter = createRecordWriter(jobApplySchemaFile,avroFile);

        org.apache.avro.Schema schemaJSON = org.apache.avro.Schema.parse(getJSONSchema(Schema.fromFile(new File(jobApplySchemaFile))).toString());

        for(int i=0;i<_numRecords;i++)
        {
            final GenericData.Record outRecord = new GenericData.Record(schemaJSON);

            GenericRow applicantProfile = getRandomGenericRow(profileTable);
            GenericRow jobInfo = getRandomGenericRow(jobTable);

            outRecord.put("ApplyStartTime", eventTimeGenerator.next());
            outRecord.put("TimeSpent", timeSpentGenerator.next());
            outRecord.put("JobSalary", jobInfo.getValue("Salary"));
            outRecord.put("ApplicantProfileId", applicantProfile.getValue("ID"));
            outRecord.put("ApplicantHeadline", applicantProfile.getValue("Headline"));
            outRecord.put("ApplicantPosition", applicantProfile.getValue("Position"));
            outRecord.put("JobID", jobInfo.getValue("ID"));
            outRecord.put("JobUrl", jobInfo.getValue("Url"));
            outRecord.put("JobTitle", jobInfo.getValue("Title"));
            outRecord.put("JobCompany", jobInfo.getValue("Company"));
            outRecord.put("DidApplyIsFinalized", randomYesOrNo());

            recordWriter.append(outRecord);
        }

        recordWriter.close();
        return true;
    }

    public boolean generateArticleReadTable(long timeColumnStart, long timeColumnEnd, int numRecords) throws Exception
    {
        ClassLoader classLoader = EventTableGenerator.class.getClassLoader();
        String profileSchemaFile = getFileFromResourceUrl(classLoader.getResource(_profileSchemaFile));
        String articleSchemaFile = getFileFromResourceUrl(classLoader.getResource(_articleSchemaFile));
        String articleReadSchemaAnn = getFileFromResourceUrl(classLoader.getResource(_articleReadSchemaAnnFile));
        String articleReadSchemaFile = getFileFromResourceUrl(classLoader.getResource(_articleReadSchemaFile));

        String profileDataFile = getTableDataDirectory("profile");
        List<GenericRow> profileTable = readBaseTableData(profileSchemaFile,profileDataFile);

        String articleDataFile = getTableDataDirectory("article");
        List<GenericRow> articleTable = readBaseTableData(articleSchemaFile, articleDataFile);

        List<SchemaAnnotation> saList = readSchemaAnnotationFile(articleReadSchemaAnn);
        RangeLongGenerator eventTimeGenerator = new RangeLongGenerator(timeColumnStart, timeColumnEnd);
        RangeIntGenerator timeSpentGenerator = createIntRangeGenerator(saList,"TimeSpent");

        File avroFile = createOutDirAndFile("ArticleRead");
        DataFileWriter<GenericData.Record> recordWriter = createRecordWriter(articleReadSchemaFile,avroFile);

        org.apache.avro.Schema schemaJSON = org.apache.avro.Schema.parse(getJSONSchema(Schema.fromFile(new File(articleReadSchemaFile))).toString());

        for(int i=0;i<numRecords;i++)
        {
            final GenericData.Record outRecord = new GenericData.Record(schemaJSON);

            GenericRow readerProfile = getRandomGenericRow(profileTable);
            GenericRow articleInfo = getRandomGenericRow(articleTable);

            outRecord.put("ReadStartTime", eventTimeGenerator.next());
            outRecord.put("TimeSpent", timeSpentGenerator.next());
            outRecord.put("ReaderStrength", readerProfile.getValue("Strength"));
            outRecord.put("ReaderProfileId", readerProfile.getValue("ID"));
            outRecord.put("ReaderHeadline", readerProfile.getValue("Headline"));
            outRecord.put("ReaderPosition", readerProfile.getValue("Position"));
            outRecord.put("ArticleID", articleInfo.getValue("ID"));
            outRecord.put("ArticleUrl", articleInfo.getValue("Url"));
            outRecord.put("ArticleTitle", articleInfo.getValue("Title"));
            outRecord.put("ArticleAbstract", articleInfo.getValue("Abstract"));
            outRecord.put("ArticleAuthor", articleInfo.getValue("Author"));
            outRecord.put("ArticleCompany", articleInfo.getValue("Company"));
            outRecord.put("ArticleTopic", articleInfo.getValue("Topic"));

            recordWriter.append(outRecord);
        }

        recordWriter.close();
        return true;
    }


    public boolean generateArticleReadTable() throws Exception
    {
        ClassLoader classLoader = EventTableGenerator.class.getClassLoader();
        String profileSchemaFile = getFileFromResourceUrl(classLoader.getResource(_profileSchemaFile));
        String articleSchemaFile = getFileFromResourceUrl(classLoader.getResource(_articleSchemaFile));
        String articleReadSchemaAnn = getFileFromResourceUrl(classLoader.getResource(_articleReadSchemaAnnFile));
        String articleReadSchemaFile = getFileFromResourceUrl(classLoader.getResource(_articleReadSchemaFile));

        String profileDataFile = getTableDataDirectory("profile");
        List<GenericRow> profileTable = readBaseTableData(profileSchemaFile,profileDataFile);

        String articleDataFile = getTableDataDirectory("article");
        List<GenericRow> articleTable = readBaseTableData(articleSchemaFile, articleDataFile);

        List<SchemaAnnotation> saList = readSchemaAnnotationFile(articleReadSchemaAnn);
        RangeLongGenerator eventTimeGenerator = createLongRangeGenerator(saList,"ReadStartTime");
        RangeIntGenerator timeSpentGenerator = createIntRangeGenerator(saList,"TimeSpent");

        File avroFile = createOutDirAndFile("ArticleRead");
        DataFileWriter<GenericData.Record> recordWriter = createRecordWriter(articleReadSchemaFile,avroFile);

        org.apache.avro.Schema schemaJSON = org.apache.avro.Schema.parse(getJSONSchema(Schema.fromFile(new File(articleReadSchemaFile))).toString());

        for(int i=0;i<_numRecords;i++)
        {
            final GenericData.Record outRecord = new GenericData.Record(schemaJSON);

            GenericRow readerProfile = getRandomGenericRow(profileTable);
            GenericRow articleInfo = getRandomGenericRow(articleTable);

            outRecord.put("ReadStartTime", eventTimeGenerator.next());
            outRecord.put("TimeSpent", timeSpentGenerator.next());
            outRecord.put("ReaderStrength", readerProfile.getValue("Strength"));
            outRecord.put("ReaderProfileId", readerProfile.getValue("ID"));
            outRecord.put("ReaderHeadline", readerProfile.getValue("Headline"));
            outRecord.put("ReaderPosition", readerProfile.getValue("Position"));
            outRecord.put("ArticleID", articleInfo.getValue("ID"));
            outRecord.put("ArticleUrl", articleInfo.getValue("Url"));
            outRecord.put("ArticleTitle", articleInfo.getValue("Title"));
            outRecord.put("ArticleAbstract", articleInfo.getValue("Abstract"));
            outRecord.put("ArticleAuthor", articleInfo.getValue("Author"));
            outRecord.put("ArticleCompany", articleInfo.getValue("Company"));
            outRecord.put("ArticleTopic", articleInfo.getValue("Topic"));

            recordWriter.append(outRecord);
        }

        recordWriter.close();
        return true;
    }


    private String randomYesOrNo()
    {
        Random randGen = new Random(System.currentTimeMillis());
        int index = randGen.nextInt(2);
        if(index == 1)
            return "Yes";
        else
            return "NO";
    }

    public GenericRow getRandomGenericRow(List<GenericRow> rowList)
    {
        int size = rowList.size();
        Random randGen = new Random(System.currentTimeMillis());
        int index = randGen.nextInt(size);
        return rowList.get(index);
    }

    public JSONObject getJSONSchema(Schema schema) throws JSONException {
        final JSONObject ret = new JSONObject();
        ret.put("name", "data_gen_record");
        ret.put("type", "record");

        final JSONArray fields = new JSONArray();

        for (final FieldSpec spec : schema.getAllFieldSpecs()) {
            fields.put(spec.getDataType().toJSONSchemaFor(spec.getName()));
        }

        ret.put("fields", fields);

        return ret;
    }

    public static void main(String[] args) throws Exception
    {
        String str = "Jun 13 2003 23:11:52.454 UTC";
        SimpleDateFormat df = new SimpleDateFormat("MMM dd yyyy HH:mm:ss.SSS zzz");
        Date date = df.parse(str);
        long epoch = date.getTime();
        System.out.println(epoch); // 1055545912454
    }
}

