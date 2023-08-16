package org.apache.pinot.segment.local.segment.creator.impl.vec;

import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.VectorIndexCreator;
import org.apache.pinot.spi.data.readers.Vector;
import org.roaringbitmap.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.TreeMap;

public abstract class BaseVectorIndexCreator implements VectorIndexCreator {
  public static final int VERSION = 1;
  public static final int HEADER_LENGTH = 10;

  static final String TEMP_DIR_SUFFIX = V1Constants.Indexes.RAW_VECTOR_INDEX_FILE_EXTENSION + ".tmp";
  static final String DICTIONARY_FILE_NAME = "dictionary.buf";
  static final String BITMAP_OFFSET_FILE_NAME = "bitmap.offset.buf";
  static final String BITMAP_VALUE_FILE_NAME = "bitmap.value.buf";

  final File _indexFile;
  final File _tempDir;
  final File _dictionaryFile;
  final File _bitmapOffsetFile;
  final File _bitmapValueFile;
  final DataOutputStream _dictionaryStream;
  final DataOutputStream _bitmapOffsetStream;
  final DataOutputStream _bitmapValueStream;

  final Map<Vector, RoaringBitmapWriter<RoaringBitmap>> _postingListMap = new TreeMap<>();
  final RoaringBitmapWriter.Wizard<Container, RoaringBitmap> _bitmapWriterWizard =
      RoaringBitmapWriter.writer().runCompress(false);

  int _nextDocId;

  BaseVectorIndexCreator(File indexDir, String columnName) throws IOException {
    _indexFile = new File(indexDir, columnName + V1Constants.Indexes.RAW_VECTOR_INDEX_FILE_EXTENSION);
    _tempDir = new File(indexDir, columnName + TEMP_DIR_SUFFIX);
    if (_tempDir.exists()) {
      FileUtils.cleanDirectory(_tempDir);
    } else {
      FileUtils.forceMkdir(_tempDir);
    }
    _dictionaryFile = new File(_tempDir, DICTIONARY_FILE_NAME);
    _bitmapOffsetFile = new File(_tempDir, BITMAP_OFFSET_FILE_NAME);
    _bitmapValueFile = new File(_tempDir, BITMAP_VALUE_FILE_NAME);
    _dictionaryStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(_dictionaryFile)));
    _bitmapOffsetStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(_bitmapOffsetFile)));
    _bitmapValueStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(_bitmapValueFile)));
  }

  public void add(Vector vector){
    RoaringBitmapWriter<RoaringBitmap> bitmapWriter = _postingListMap.get(vector);
    if (bitmapWriter == null) {
      bitmapWriter = _bitmapWriterWizard.get();
      _postingListMap.put(vector, bitmapWriter);
    }
    bitmapWriter.add(_nextDocId++);
  }

  void add(Vector vector, BitmapDataProvider bitmap) throws IOException {
    _dictionaryStream.write(vector.toBytes()); // Using the toBytes function directly
    _bitmapOffsetStream.writeInt(_bitmapValueStream.size());
    bitmap.serialize(_bitmapValueStream);
  }

  void generateIndexFile() throws IOException {
    _bitmapOffsetStream.writeInt(_bitmapValueStream.size());

    _dictionaryStream.close();
    _bitmapOffsetStream.close();
    _bitmapValueStream.close();

    ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_LENGTH);
    headerBuffer.putInt(VERSION);
    headerBuffer.putInt(_dictionaryStream.size());  // Adjusted based on the serialized vector length
    headerBuffer.position(0);

    try (FileChannel indexFileChannel = new RandomAccessFile(_indexFile, "rw").getChannel();
        FileChannel dictionaryFileChannel = new RandomAccessFile(_dictionaryFile, "r").getChannel();
        FileChannel bitmapOffsetFileChannel = new RandomAccessFile(_bitmapOffsetFile, "r").getChannel();
        FileChannel bitmapValueFileChannel = new RandomAccessFile(_bitmapValueFile, "r").getChannel()) {
      indexFileChannel.write(headerBuffer);
      org.apache.pinot.common.utils.FileUtils.transferBytes(dictionaryFileChannel, 0, _dictionaryFile.length(),
          indexFileChannel);
      org.apache.pinot.common.utils.FileUtils.transferBytes(bitmapOffsetFileChannel, 0, _bitmapOffsetFile.length(),
          indexFileChannel);
      org.apache.pinot.common.utils.FileUtils.transferBytes(bitmapValueFileChannel, 0, _bitmapValueFile.length(),
          indexFileChannel);
    }
  }

  public void close() throws IOException {
    _dictionaryStream.close();
    _bitmapOffsetStream.close();
    _bitmapValueStream.close();

    FileUtils.deleteDirectory(_tempDir);
  }
}
