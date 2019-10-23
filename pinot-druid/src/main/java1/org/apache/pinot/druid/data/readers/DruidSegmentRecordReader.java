package org.apache.pinot.druid.data.readers;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.query.DruidProcessingConfig;

import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.data.readers.RecordReader;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;

import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The DruidSegmentRecordReader allows us to convert all of the rows in a Druid segment file
 * into GenericRows, which are made into Pinot segments.
 */
public class DruidSegmentRecordReader implements RecordReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(DruidSegmentRecordReader.class);

  private Schema _pinotSchema;
  private Cursor _cursor;
  private ArrayList<String> _columnNames;
  private List<BaseObjectColumnValueSelector> _selectors;

  private void init(String indexPath, Schema schema)
      throws IOException {

    // Only the columns whose names are in the Pinot schema will get processed
    _pinotSchema = schema;
    // getColumnNames() puts the column names in the schema into a Set
    // so the columns will end up in a random order.
    // Consider a different implementation where the order is consistent.
    _columnNames = new ArrayList<>();
    _columnNames.addAll(_pinotSchema.getColumnNames());

    ColumnConfig config = new DruidProcessingConfig()
    {
      @Override
      public String getFormatString()
      {
        return "processing-%s";
      }

      @Override
      public int intermediateComputeSizeBytes()
      {
        return 100 * 1024 * 1024;
      }

      @Override
      public int getNumThreads()
      {
        return 1;
      }

      @Override
      public int columnCacheSizeBytes()
      {
        return 25 * 1024 * 1024;
      }
    };

    ObjectMapper mapper = new DefaultObjectMapper();
    final IndexIO indexIO = new IndexIO(mapper, config);

    try {
      QueryableIndex index = indexIO.loadIndex(new File(indexPath));
      LOGGER.info("Creating segment from path: {}", indexPath);
      QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(index);

      // A Sequence "represents an iterable sequence of elements. Unlike normal Iterators however, it doesn't expose
      // a way for you to extract values from it, instead you provide it with a worker (an Accumulator) and that defines
      // what happens with the data."
      final Sequence<Cursor> cursors = adapter.makeCursors(
          Filters.toFilter(null),
          index.getDataInterval().withChronology(ISOChronology.getInstanceUTC()),
          VirtualColumns.EMPTY,
          Granularities.ALL,
          false,
          null
      );
      // Turn cursors into a Yielder
      // A Yielder is an object like a linked list where the Yielder lets you access the current value with get()
      // and lets you advance to the next item in the chain with next().
      // A Yielder isDone() when there is nothing else in the chain.
      // Also consider accumulating the Sequence instead of turning it into a Yielder
      YieldingAccumulator<Cursor, Cursor> accumulator = new YieldingAccumulator<Cursor, Cursor>() {
        @Override
        public Cursor accumulate(Cursor accumulated, Cursor in) {
          return in;
        }
      };
      Yielder<Cursor> cursorYielder = cursors.toYielder(null, accumulator);
      _cursor = cursorYielder.get();
      // A Yielder must be closed to prevent resource leaks.
      cursorYielder.close();

      ColumnSelectorFactory columnSelectorFactory = _cursor.getColumnSelectorFactory();
      _selectors = _columnNames
          .stream()
          .map(columnSelectorFactory::makeColumnValueSelector)
          .collect(Collectors.toList());

    } catch (Exception e) {
    e.printStackTrace();
    }
  }

  @Override
  public void init(SegmentGeneratorConfig segmentGeneratorConfig)
      throws IOException {
    init(segmentGeneratorConfig.getInputFilePath(), segmentGeneratorConfig.getSchema());
  }

  @Override
  public boolean hasNext() {
    return !_cursor.isDone();
  }

  @Override
  public GenericRow next() throws IOException {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse) throws IOException {
    // TODO: use Pinot schema to fill the values to handle missing column and default values properly
    // Only the columns whose names are in the Pinot schema will get processed
    for (int i = 0; i < _columnNames.size(); i++) {
      final String columnName = _columnNames.get(i);
      final Object value = _selectors.get(i).getObject();
      // Figure out how Pinot handles time; see TimeGranularitySpec and TimeFieldSpec
      if (_columnNames.get(i).equals(ColumnHolder.TIME_COLUMN_NAME)) {
        reuse.putField(columnName, new DateTime());
      }
      reuse.putField(columnName, value);
    }
    _cursor.advance();
    return reuse;
  }

  @Override
  public void rewind()
      throws IOException {
    // Apparently this never throws an IOException
    _cursor.reset();
  }

  @Override
  public Schema getSchema() {
    return _pinotSchema;
  }

  @Override
  public void close() throws IOException {
    // Not sure what to close; the Yielder was the only Closeable.
  }
}