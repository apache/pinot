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

package com.linkedin.pinot.core.startreeV2;


import java.io.File;
import java.util.List;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.io.FileNotFoundException;
import java.nio.channels.FileChannel;
import com.linkedin.pinot.common.segment.StarTreeV2Metadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;


public class StarTreeV1ToV2Converter implements StarTreeFormatConverter {

  private List<StarTreeV2Metadata> _starTreeV2MetadataList;

  @Override
  public void convert(File indexStarTreeDir, int starTreeId) throws Exception {

    SegmentMetadataImpl v2Metadata = new SegmentMetadataImpl(indexStarTreeDir);
    createFilesIfNotExist(indexStarTreeDir);
    copyIndexData(indexStarTreeDir, v2Metadata, starTreeId);

    return;
  }

  private void copyIndexData(File v2Directory, SegmentMetadataImpl v2Metadata, int starTreeId)
      throws Exception {

    _starTreeV2MetadataList = v2Metadata.getStarTreeV2Metadata();
    StarTreeV2Metadata metaData = _starTreeV2MetadataList.get(starTreeId);

    List<String> dimensionsSplitOrder = metaData.getDimensionsSplitOrder();
    List<String> met2aggfuncPairs = metaData.getMet2AggfuncPairs();

    for (String dimension: dimensionsSplitOrder) {
      appendToFile(v2Directory, dimension, "DIMENSION", starTreeId);
    }

    //readFromFile(v2Directory);

    for (String pair: met2aggfuncPairs) {
      appendToFile(v2Directory, pair, "METRIC", starTreeId);
    }

    return;
  }

  private void createFilesIfNotExist(File indexStarTreeDir) throws IOException {

    File index_file = new File(indexStarTreeDir, StarTreeV2Constant.STAR_TREE_V2_INDEX_MAP_FILE);
    if (!index_file.exists()) {
      index_file.createNewFile();
    }

    File data_file = new File(indexStarTreeDir, StarTreeV2Constant.STAR_TREE_V2_COlUMN_FILE);
    if (!data_file.exists()) {
      data_file.createNewFile();
    }

    File temp_file = new File(indexStarTreeDir, "temp.tmp");
    if (!temp_file.exists()) {
      temp_file.createNewFile();
    }
    return;
  }

  private void appendToFile(File path, String column, String type, int starTreeId) throws IOException {
    String readerFile = "";
    if (type.equals("DIMENSION")) {
      readerFile = new File(path, column + StarTreeV2Constant.DIMENSION_FWD_INDEX_SUFFIX).getPath();
    } else {
      readerFile = new File(path, column + StarTreeV2Constant.METRIC_RAW_INDEX_SUFFIX).getPath();
    }

    String indexWriterFile = new File(path, StarTreeV2Constant.STAR_TREE_V2_INDEX_MAP_FILE).getPath();
    String columnWriterFile = new File(path, StarTreeV2Constant.STAR_TREE_V2_COlUMN_FILE).getPath();

    try {
      int b;
      File inFile = new File(readerFile);
      ByteBuffer buf = ByteBuffer.allocateDirect((int)inFile.length());
      InputStream is = new FileInputStream(inFile);
      while ((b=is.read())!=-1) {
        buf.put((byte)b);
      }

      File file = new File(columnWriterFile);
      boolean append = true;
      FileChannel channel = new FileOutputStream(file, append).getChannel();
      long start = channel.size();
      buf.flip();
      long end = start + buf.remaining();
      channel.write(buf);
      channel.close();

      PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(indexWriterFile, true)));
      out.println("startree" + starTreeId + "." + column + ".start:" + start);
      out.println("startree" + starTreeId + "." + column + ".end:" + end);
      out.close();

    }
    catch(FileNotFoundException ex) {
      System.out.println("Unable to open file '" + indexWriterFile + "'");
    }
    catch(IOException ex) {
      System.out.println("Error reading file '" + readerFile + "'");
    }
  }

  private void readFromFile(File path) throws IOException {
    String[] column = new String[10];

    try {
      String indexWriterFile = new File(path, StarTreeV2Constant.STAR_TREE_V2_INDEX_MAP_FILE).getPath();
      File file = new File(indexWriterFile);
      FileReader fileReader = new FileReader(file);
      BufferedReader bufferedReader = new BufferedReader(fileReader);
      StringBuffer stringBuffer = new StringBuffer();
      String line;

      int i = 0;
      while ((line = bufferedReader.readLine()) != null) {
        column[i] = line.toString();
        i = i + 1;
        System.out.println(line.toString());
      }
      fileReader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    String columnWriterFile = new File(path, StarTreeV2Constant.STAR_TREE_V2_COlUMN_FILE).getPath();

    for ( int i = 0; i < column.length; i += 2) {
      String a = column[i];
      String b = column[i+1];

      String [] aparts = a.split(":");
      String [] bparts = b.split(":");

      long start = Integer.valueOf(aparts[1]);
      long end = Integer.valueOf(bparts[1]);

      byte[] magic = new byte[(int) end];
      RandomAccessFile raf = new RandomAccessFile(columnWriterFile, "rw");
      raf.seek(start);
      raf.readFully(magic);
      System.out.println(new String(magic));


    }
  }
}
