/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.kohsuke.args4j.Option;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.tools.data.generator.DataGenerator;
import com.linkedin.pinot.tools.data.generator.DataGeneratorSpec;

/**
 * Class to implement GenerateData command.
 *
 * @author Mayank Shrivastava <mshrivastava@linkedin.com>
 */
public class GenerateDataCommand implements Command {
  @Option(name="-numRecords", required=true, metaVar="<int>", usage="number of Records to generate")
  int _numRecords = 0;

  @Option(name="-numFiles", required=true, metaVar="<int>", usage="number of Records to generate")
  int _numFiles = 0;

  @Option(name="-schemaFile", required=true, metaVar= "<schemaFile>", usage="file containing schema for data")
  String _schemaFile = null;

  @Option(name="-outDir", required=true, metaVar= "<outputDir>", usage="directory where data would be generated")
  String _outDir = null;

  public void init(int numRecords, int numFiles, String schemaFile, String outDir) {
    _numRecords = numRecords;
    _numFiles = numFiles;

    _schemaFile = schemaFile;
    _outDir = outDir;
  }

  @Override
  public String toString() {
    return ("GenerateDataCommand " + _numRecords + " " + _outDir + " " + _schemaFile);
  }

  @Override
  public boolean execute() throws Exception {
    File schemaFile = new File(_schemaFile);
    Schema schema = new ObjectMapper().readValue(schemaFile, Schema.class);

    List<String> columns = new LinkedList<String>();
    final Map<String, DataType> dataTypes = new HashMap<String, FieldSpec.DataType>();
    final Map<String, Integer> cardinality = new HashMap<String, Integer>();

    // TODO: Currently, this only handles dimensions.
    // Need to enhance DataGeneratorSpec to take range for metric and time.
    for (final FieldSpec fs: schema.getAllFieldSpecs()) {
      if (fs.getFieldType() == FieldType.DIMENSION) {
        String col = fs.getName();

        columns.add(col);
        dataTypes.put(col, fs.getDataType());

        // TODO: Use random values for cardinality instead of hard-coded value.
        cardinality.put(col, 1000);
      }
    }

    final DataGeneratorSpec spec = new DataGeneratorSpec(columns, cardinality, dataTypes,
        FileFormat.AVRO, _outDir, true);

    final DataGenerator gen = new DataGenerator();
    gen.init(spec);
    gen.generate(_numRecords, _numFiles);

    return true;
  }
}
