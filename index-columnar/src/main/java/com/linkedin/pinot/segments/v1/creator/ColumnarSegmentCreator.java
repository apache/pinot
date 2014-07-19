package com.linkedin.pinot.segments.v1.creator;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.linkedin.pinot.index.data.FieldSpec;
import com.linkedin.pinot.index.data.GenericRow;
import com.linkedin.pinot.index.data.Schema;
import com.linkedin.pinot.index.segment.IndexSegment;
import com.linkedin.pinot.raw.record.readers.RecordReader;
import com.linkedin.pinot.segments.creator.SegmentCreator;
import com.linkedin.pinot.segments.generator.SegmentGeneratorConfiguration;
import com.linkedin.pinot.segments.v1.segment.dictionary.Dictionary;


public class ColumnarSegmentCreator implements SegmentCreator {

  private SegmentGeneratorConfiguration config;
  private RecordReader reader;
  private Map<String, DictionaryCreator> dictionaryCreatorsMap;
  private Map<String, Dictionary<?>> dictionaryMap;
  private Map<String, IndexesCreator> indexCreatorMap;
  private Schema dataSchema;
  private File indexDir;

  public ColumnarSegmentCreator() {

  }

  public ColumnarSegmentCreator(RecordReader recordReader) {
    this.reader = recordReader;
    this.dictionaryCreatorsMap = new HashMap<String, DictionaryCreator>();
    this.dictionaryMap = new HashMap<String, Dictionary<?>>();
    this.indexCreatorMap = new HashMap<String, IndexesCreator>();
    this.dataSchema = recordReader.getSchema();
  }

  @Override
  public void init(SegmentGeneratorConfiguration segmentCreationSpec) {
    this.config = segmentCreationSpec;
    this.indexDir = new File(config.getOutputDir());
    indexDir.mkdir();
  }

  public void addRow(GenericRow genericRow) {
    // TODO Auto-generated method stub
    System.out.println(genericRow);
  }

  @Override
  public IndexSegment buildSegment() throws Exception {
    // TODO Auto-generated method stub

    System.out.println("creating dictionary creators");
    for (FieldSpec spec : dataSchema.getAllFieldSpecs()) {
      dictionaryCreatorsMap.put(spec.getName(), new DictionaryCreator(spec, indexDir));
    }

    System.out.println("creating dictionaries");
    while (reader.hasNext()) {
      GenericRow row = reader.next();
      for (FieldSpec spec : dataSchema.getAllFieldSpecs()) {
        dictionaryCreatorsMap.get(spec.getName()).add(row.getValue(spec.getName()));
      }
    }

    reader.close();
    reader.rewind();

    for (FieldSpec spec : dataSchema.getAllFieldSpecs()) {
      dictionaryMap.put(spec.getName(), dictionaryCreatorsMap.get(spec.getName()).seal());
    }

    for (FieldSpec spec : dataSchema.getAllFieldSpecs()) {
      indexCreatorMap.put(spec.getName(), new IndexesCreator(indexDir, dictionaryMap.get(spec.getName()), spec));
    }


    while (reader.hasNext()) {
      GenericRow row = reader.next();
      for (FieldSpec spec : dataSchema.getAllFieldSpecs()) {

      }
    }

    System.out.println("built");
    return null;
  }
}
