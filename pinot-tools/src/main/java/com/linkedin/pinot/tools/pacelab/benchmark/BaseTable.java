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
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.tools.data.generator.DataGenerator;
import com.linkedin.pinot.tools.data.generator.DataGeneratorSpec;
import com.linkedin.pinot.tools.data.generator.SchemaAnnotation;
import org.apache.commons.lang.math.IntRange;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.kohsuke.args4j.Option;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class BaseTable
{
    private String _schemaFile = null;
    private String _schemaAnotationFile = null;
    private String _outDir = null;
    private  boolean _overwrite;
    private int _numRecords = 0;
    private int _numFiles = 0;

    public BaseTable(String schemaFile, String schemaAnotationFile)
    {
        _schemaFile = schemaFile;
        _schemaAnotationFile = schemaAnotationFile;
    }

    public  boolean GenerateRandomRecords (String outFile) throws Exception
    {

        Schema schema = Schema.fromFile(new File(_schemaFile));
        List<String> columns = new LinkedList<String>();
        final HashMap<String, FieldSpec.DataType> dataTypes = new HashMap<String, FieldSpec.DataType>();
        final HashMap<String, FieldSpec.FieldType> fieldTypes = new HashMap<String, FieldSpec.FieldType>();
        final HashMap<String, TimeUnit> timeUnits = new HashMap<String, TimeUnit>();

        final HashMap<String, Integer> cardinality = new HashMap<String, Integer>();
        final HashMap<String, IntRange> range = new HashMap<String, IntRange>();

        buildCardinalityRangeMaps(_schemaAnotationFile, cardinality, range);
        final DataGeneratorSpec spec = buildDataGeneratorSpec(schema, columns, dataTypes, fieldTypes,
                timeUnits, cardinality, range);

        final DataGenerator gen = new DataGenerator();
        gen.init(spec);
        gen.generate(_numRecords, _numFiles);
        return true;
    }

    private void buildCardinalityRangeMaps(String file, HashMap<String, Integer> cardinality,
                                           HashMap<String, IntRange> range) throws JsonParseException, JsonMappingException, IOException {
        List<SchemaAnnotation> saList;

        if (file == null) {
            return; // Nothing to do here.
        }

        ObjectMapper objectMapper = new ObjectMapper();
        saList = objectMapper.readValue(new File(file), new TypeReference<List<SchemaAnnotation>>() {});

        for (SchemaAnnotation sa : saList) {
            String column = sa.getColumn();

            if (sa.isRange()) {
                range.put(column, new IntRange(sa.getRangeStart(), sa.getRangeEnd()));
            } else {
                cardinality.put(column, sa.getCardinality());
            }
        }
    }

    private DataGeneratorSpec buildDataGeneratorSpec(Schema schema, List<String> columns,
                                                     HashMap<String, FieldSpec.DataType> dataTypes, HashMap<String, FieldSpec.FieldType> fieldTypes,
                                                     HashMap<String, TimeUnit> timeUnits, HashMap<String, Integer> cardinality,
                                                     HashMap<String, IntRange> range) {
        for (final FieldSpec fs : schema.getAllFieldSpecs()) {
            String col = fs.getName();

            columns.add(col);
            dataTypes.put(col, fs.getDataType());
            fieldTypes.put(col, fs.getFieldType());

            switch (fs.getFieldType()) {
                case DIMENSION:
                    if (cardinality.get(col) == null) {
                        cardinality.put(col, 1000);
                    }
                    break;

                case METRIC:
                    if (!range.containsKey(col)) {
                        range.put(col, new IntRange(1, 1000));
                    }
                    break;

                case TIME:
                    if (!range.containsKey(col)) {
                        range.put(col, new IntRange(1, 1000));
                    }
                    TimeFieldSpec tfs = (TimeFieldSpec) fs;
                    timeUnits.put(col, tfs.getIncomingGranularitySpec().getTimeType());
                    break;

                default:
                    throw new RuntimeException("Invalid field type.");
            }
        }

        return new DataGeneratorSpec(columns, cardinality, range, dataTypes, fieldTypes,
                timeUnits, FileFormat.AVRO, _outDir, _overwrite);
    }

    protected String getIdColumn()
    {
        return  null;
    }
}
