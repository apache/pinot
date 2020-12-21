package org.apache.pinot.core.segment.creator.impl.geospatial;

import com.uber.h3core.H3Core;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.TreeMap;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.geospatial.GeometryUtils;
import org.apache.pinot.core.segment.creator.GeoSpatialIndexCreator;
import org.apache.pinot.core.segment.index.readers.geospatial.H3IndexReader;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import static org.apache.pinot.core.segment.creator.impl.V1Constants.Indexes.H3_INDEX_FILE_EXTENSION;


public class H3IndexCreator implements GeoSpatialIndexCreator {

  private static final int VERSION = 1;
  private static final int FLUSH_THRESHOLD = 100_000;
  private final H3Core _h3Core;
  private File _indexDir;
  private FieldSpec _fieldSpec;
  private int _resolution;

  TreeMap<Long, MutableRoaringBitmap> _h3IndexMap;

  int numChunks = 0;
  List<Integer> chunkLengths = new ArrayList<>();

  public H3IndexCreator(File indexDir, FieldSpec fieldSpec, int resolution)
      throws IOException {

    _indexDir = indexDir;
    _fieldSpec = fieldSpec;
    _resolution = resolution;
    _h3Core = H3Core.newInstance();
    //todo: initialize this with right size based on the
    long numHexagons = _h3Core.numHexagons(resolution);
    _h3IndexMap = new TreeMap<>();
  }

  @Override
  public void add(int docId, double lat, double lon) {
    Long h3Id = _h3Core.geoToH3(lat, lon, _resolution);
    MutableRoaringBitmap roaringBitmap = _h3IndexMap.get(h3Id);
    if (roaringBitmap == null) {
      roaringBitmap = new MutableRoaringBitmap();
      _h3IndexMap.put(h3Id, roaringBitmap);
    }
    roaringBitmap.add(docId);
    if (_h3IndexMap.size() > FLUSH_THRESHOLD) {
      flush();
    }
  }

  private void flush() {
    //dump what ever we have in _h3IndexMap in a sorted order
    try {

      File tempChunkFile = new File(_indexDir, "chunk-" + numChunks);
      DataOutputStream dos = new DataOutputStream(new FileOutputStream(tempChunkFile));
      chunkLengths.add(_h3IndexMap.size());
      for (Map.Entry<Long, MutableRoaringBitmap> entry : _h3IndexMap.entrySet()) {
        Long h3Id = entry.getKey();
        MutableRoaringBitmap bitmap = entry.getValue();
        dos.writeLong(h3Id);
        //write bitmap
        int serializedSizeInBytes = bitmap.serializedSizeInBytes();
        byte[] byteArray = new byte[serializedSizeInBytes];
        bitmap.serialize(ByteBuffer.wrap(byteArray));
        dos.writeInt(serializedSizeInBytes);
        dos.write(byteArray);
      }
      dos.close();
      _h3IndexMap.clear();
      numChunks++;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void seal()
      throws Exception {
    flush();

    //merge all the chunk files, since they are sorted we can write the dictionary as well
    PriorityQueue<Entry> queue = new PriorityQueue<>();

    ChunkReader[] chunkReaders = new ChunkReader[numChunks];
    for (int chunkId = 0; chunkId < numChunks; chunkId++) {
      File chunkFile = new File(_indexDir, "chunk-" + chunkId);
      chunkReaders[chunkId] = new ChunkReader(chunkId, chunkLengths.get(chunkId), chunkFile);

      Entry e = chunkReaders[chunkId].getNextEntry();
      queue.add(e);
    }
    long prevH3Id = -1;
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();

    File headerFile = new File(_indexDir, _fieldSpec.getName() + "-h3index-header.buffer");
    DataOutputStream headerStream = new DataOutputStream(new FileOutputStream(headerFile));

    File dictionaryFile = new File(_indexDir, _fieldSpec.getName() + "-h3index-dictionary.buffer");
    DataOutputStream dictionaryStream = new DataOutputStream(new FileOutputStream(dictionaryFile));

    File offsetFile = new File(_indexDir, _fieldSpec.getName() + "-h3index-offset.buffer");
    DataOutputStream offsetStream = new DataOutputStream(new FileOutputStream(offsetFile));

    File bitmapFile = new File(_indexDir, _fieldSpec.getName() + "-h3index-bitmap.buffer");
    DataOutputStream bitmapStream = new DataOutputStream(new FileOutputStream(bitmapFile));

    Writer writer = new Writer(dictionaryStream, offsetStream, bitmapStream);
    while (queue.size() > 0) {
      Entry poll = queue.poll();
      long currH3Id = poll.h3Id;
      if (prevH3Id != -1 && currH3Id != prevH3Id) {
        writer.add(prevH3Id, bitmap);
        bitmap.clear();
      }
      bitmap.or(poll.bitmap);

      prevH3Id = currH3Id;

      Entry e = chunkReaders[poll.chunkId].getNextEntry();
      if (e != null) {
        queue.add(e);
      }
    }
    if (prevH3Id != -1) {
      writer.add(prevH3Id, bitmap);
    }

    //write header file
    headerStream.writeInt(VERSION);
    headerStream.writeInt(writer.getNumUniqueIds());
    headerStream.close();
    dictionaryStream.close();
    offsetStream.close();
    bitmapStream.close();

    File outputFile = new File(_indexDir, _fieldSpec.getName() + H3_INDEX_FILE_EXTENSION);
    long length = headerStream.size() + dictionaryStream.size() + offsetStream.size() + bitmapStream.size();
    //write the actual file
    PinotDataBuffer h3IndexBuffer =
        PinotDataBuffer.mapFile(outputFile, false, 0, length, ByteOrder.BIG_ENDIAN, "H3 Index Buffer");

    long writtenBytes = 0;
    h3IndexBuffer.readFrom(writtenBytes, headerFile, 0, headerFile.length());
    writtenBytes += headerFile.length();

    h3IndexBuffer.readFrom(writtenBytes, dictionaryFile, 0, dictionaryFile.length());
    writtenBytes += dictionaryFile.length();

    h3IndexBuffer.readFrom(writtenBytes, offsetFile, 0, offsetFile.length());
    writtenBytes += offsetFile.length();

    h3IndexBuffer.readFrom(writtenBytes, bitmapFile, 0, bitmapFile.length());
    writtenBytes += headerFile.length();
  }

  @Override
  public void close()
      throws IOException {
    //delete chunk files
  }

  class ChunkReader {
    private int _chunkId;
    private Integer _chunkLength;
    private DataInputStream dataInputStream;
    int index = 0;

    ChunkReader(int chunkId, Integer chunkLength, File chunkFile)
        throws Exception {
      _chunkId = chunkId;
      _chunkLength = chunkLength;
      dataInputStream = new DataInputStream(new FileInputStream(chunkFile));
    }

    private Entry getNextEntry()
        throws IOException {
      if (index >= _chunkLength) {
        return null;
      }
      long h3Id = dataInputStream.readLong();
      int size = dataInputStream.readInt();
      byte[] serializedBytes = new byte[size];
      dataInputStream.read(serializedBytes);
      ImmutableRoaringBitmap bitmap = new ImmutableRoaringBitmap(ByteBuffer.wrap(serializedBytes));
      index++;
      return new Entry(_chunkId, h3Id, bitmap);
    }
  }

  class Entry implements Comparable<Entry> {
    int chunkId;

    long h3Id;

    ImmutableRoaringBitmap bitmap;

    @Override
    public boolean equals(Object o) {

      return h3Id == ((Entry) o).h3Id;
    }

    public Entry(int chunkId, long h3Id, ImmutableRoaringBitmap bitmap) {
      this.chunkId = chunkId;
      this.h3Id = h3Id;
      this.bitmap = bitmap;
    }

    @Override
    public int hashCode() {
      return Long.hashCode(h3Id);
    }

    @Override
    public int compareTo(Entry o) {
      return Long.compare(h3Id, o.h3Id);
    }
  }

  private class Writer {

    private DataOutputStream _dictionaryStream;
    private DataOutputStream _offsetStream;
    private DataOutputStream _bitmapStream;
    private int _offset = 0;
    private int _numUniqueIds = 0;

    public Writer(DataOutputStream dictionaryStream, DataOutputStream offsetStream, DataOutputStream bitmapStream) {

      _dictionaryStream = dictionaryStream;
      _offsetStream = offsetStream;
      _bitmapStream = bitmapStream;
    }

    public int getNumUniqueIds() {
      return _numUniqueIds;
    }

    public void add(long h3Id, ImmutableRoaringBitmap bitmap)
        throws IOException {
      _dictionaryStream.writeLong(h3Id);
      _offsetStream.writeInt(_offset);
      int serializedSizeInBytes = bitmap.serializedSizeInBytes();
      byte[] byteArray = new byte[serializedSizeInBytes];
      bitmap.serialize(ByteBuffer.wrap(byteArray));
      _bitmapStream.write(byteArray);
      _offset += serializedSizeInBytes;
      _numUniqueIds++;
    }
  }

  public static void main(String[] args)
      throws Exception {
    Point point1 = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(37.3861, -122.0839));
    Point point2 = GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(37.368832, -122.036346));
    System.out.println("point1.distance(point2) = " + point1.distance(point2));
    System.exit(0);
    File indexDir = new File(System.getProperty("java.io.tmpdir"), "h3IndexDir");
    FileUtils.deleteDirectory(indexDir);
    indexDir.mkdirs();
    FieldSpec spec = new DimensionFieldSpec("geo_col", FieldSpec.DataType.STRING, true);
    int resolution = 5;
    H3IndexCreator creator = new H3IndexCreator(indexDir, spec, resolution);
    Random rand = new Random();
    H3Core h3Core = H3Core.newInstance();
    Map<Long, Integer> map = new HashMap<>();
    for (int i = 0; i < 10000; i++) {
      int lat = rand.nextInt(10);
      int lon = rand.nextInt(10);
      creator.add(i, lat, lon);
      long h3 = h3Core.geoToH3(lat, lon, resolution);
      Integer count = map.get(h3);
      if (count != null) {
        map.put(h3, count + 1);
      } else {
        map.put(h3, 1);
      }
    }
    creator.seal();

    System.out.println(
        "Contents of IndexDir \n " + FileUtils.listFiles(indexDir, null, true).toString().replaceAll(",", "\n"));
    File h3IndexFile = new File(indexDir, "geo_col.h3.index");
    PinotDataBuffer h3IndexBuffer =
        PinotDataBuffer.mapFile(h3IndexFile, true, 0, h3IndexFile.length(), ByteOrder.BIG_ENDIAN, "H3 index file");
    H3IndexReader reader = new H3IndexReader(h3IndexBuffer);
    for (Map.Entry<Long, Integer> entry : map.entrySet()) {
      Long h3 = entry.getKey();
      ImmutableRoaringBitmap docIds = reader.getDocIds(h3);
      if (docIds.getCardinality() != map.get(h3)) {
        System.out.printf("Failed: expected: %d actual: %d for h3:%d \n", map.get(h3), docIds.getCardinality(), h3);
      } else {
        System.out.printf("Matched: expected: %d actual: %d for h3:%d \n", map.get(h3), docIds.getCardinality(), h3);
      }
    }


  }
}
