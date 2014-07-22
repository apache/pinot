package com.linkedin.pinot.segments.v1.creator;

import it.unimi.dsi.fastutil.doubles.DoubleAVLTreeSet;
import it.unimi.dsi.fastutil.doubles.DoubleBidirectionalIterator;
import it.unimi.dsi.fastutil.floats.FloatAVLTreeSet;
import it.unimi.dsi.fastutil.floats.FloatBidirectionalIterator;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntBidirectionalIterator;
import it.unimi.dsi.fastutil.longs.LongAVLTreeSet;
import it.unimi.dsi.fastutil.longs.LongBidirectionalIterator;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.linkedin.pinot.index.data.FieldSpec;
import com.linkedin.pinot.index.data.FieldSpec.DataType;
import com.linkedin.pinot.index.data.FieldSpec.FieldType;
import com.linkedin.pinot.segments.utils.InputOutputStreamUtils;
import com.linkedin.pinot.segments.v1.segment.dictionary.Dictionary;
import com.linkedin.pinot.segments.v1.segment.dictionary.mmap.MmapBooleanDictionary;
import com.linkedin.pinot.segments.v1.segment.dictionary.mmap.MmapDoubleDictionary;
import com.linkedin.pinot.segments.v1.segment.dictionary.mmap.MmapFloatDictionary;
import com.linkedin.pinot.segments.v1.segment.dictionary.mmap.MmapIntDictionary;
import com.linkedin.pinot.segments.v1.segment.dictionary.mmap.MmapLongDictionary;
import com.linkedin.pinot.segments.v1.segment.dictionary.mmap.MmapStringDictionary;


/**
 * Jun 30, 2014
 * 
 * @author Dhaval Patel <dpatel@linkedin.com>
 * @param <E>
 * 
 */

public class DictionaryCreator {

  private static Logger logger = Logger.getLogger(DictionaryCreator.class);
  // persistence information
  private File indexDir;
  private File dictionaryOutputFile;

  // typed tree setss

  private IntAVLTreeSet intAVLTreeSet;
  private FloatAVLTreeSet floatAVLTreeSet;
  private LongAVLTreeSet longAVLTreeSet;
  private DoubleAVLTreeSet doubleAVLTreeSet;
  private TreeSet<String> stringSet;
  private Dictionary<?> dictionary;

  // string padding length helper
  private int longestStringLength = 0;

  // stats collectors
  private int numDocsCounter = 0;
  private Object previousValue = null;
  private long timeTaken = 0;

  private boolean isSorted = true;
  private boolean isMultiValued = false;

  private int prevBiggerThanNextCount = 0;
  private int numberOfChanges = 0;

  private FieldSpec spec;

  // current no MV support
  public DictionaryCreator(FieldSpec spec, File indexDir) {
    this.spec = spec;
    intAVLTreeSet = new IntAVLTreeSet();
    longAVLTreeSet = new LongAVLTreeSet();
    stringSet = new TreeSet<String>();
    floatAVLTreeSet = new FloatAVLTreeSet();
    doubleAVLTreeSet = new DoubleAVLTreeSet();
    previousValue = addressNullAndConvertIfNeeded(null);
    dictionaryOutputFile = new File(indexDir, spec.getName() + V1Constants.Dict.FILE_EXTENTION);
    this.timeTaken = System.currentTimeMillis();
  }

  public void add(Object incoming) {
    if (!spec.isSingleValueField())
      return;

    numDocsCounter++;

    incoming = addressNullAndConvertIfNeeded(incoming);

    if (((Comparable) incoming).compareTo(previousValue) != 0) {
      numberOfChanges++;
    }
    if (((Comparable) incoming).compareTo(previousValue) < 0) {
      prevBiggerThanNextCount++;
    }

    if (!incoming.equals(previousValue) && previousValue != null) {
      Comparable prevValue = (Comparable) previousValue;
      Comparable origin = (Comparable) incoming;
      if (origin.compareTo(prevValue) < 0) {
        isSorted = false;
      }
    }

    previousValue = incoming;
    index(incoming);
  }

  private Object addressNullAndConvertIfNeeded(Object value) {
    switch (spec.getDataType()) {
      case INT:
        if (value == null)
          value = V1Constants.Numbers.NULL_INT;
        if (value instanceof String)
          value = new Integer(Integer.parseInt((String) value));
        break;
      case LONG:
        if (value == null)
          value = V1Constants.Numbers.NULL_LONG;
        if (value instanceof String)
          value = new Long(Long.parseLong((String) value));
        break;
      case FLOAT:
        if (value == null)
          value = V1Constants.Numbers.NULL_FLOAT;
        if (value instanceof String)
          value = new Float(Float.parseFloat((String) value));
        break;
      case DOUBLE:
        if (value == null)
          value = V1Constants.Numbers.NULL_DOUBLE;
        if (value instanceof String)
          value = new Double(Double.parseDouble((String) value));
        break;
      case BOOLEAN:
      case STRING:
        if (value == null)
          value = V1Constants.Str.NULL_STRING;
        if (value instanceof String)
          value = (String) value;
        if (value instanceof Boolean)
          value = String.valueOf((Boolean) value);
        break;
    }
    return value;
  }

  private void index(Object value) {
    switch (spec.getDataType()) {
      case INT:
        intAVLTreeSet.add((Integer) value);
        break;
      case LONG:
        longAVLTreeSet.add((Long) value);
        break;
      case FLOAT:
        floatAVLTreeSet.add((Float) value);
        break;
      case DOUBLE:
        doubleAVLTreeSet.add((Double) value);
        break;
      case BOOLEAN:
      case STRING:
        stringSet.add((String) value);
        int len = ((String) value).length();
        if (len > longestStringLength)
          longestStringLength = len;
        break;
    }
  }

  public Set<String> getStringSet() {
    return stringSet;
  }

  private void saveIntDictionary() throws IOException {
    int[] intArray = new int[intAVLTreeSet.size()];
    IntBidirectionalIterator iterator = intAVLTreeSet.iterator();
    for (int i = 0; i < intArray.length; i++) {
      intArray[i] = iterator.nextInt();
    }

    Arrays.sort(intArray);

    DataOutputStream out = InputOutputStreamUtils.getDefaultOutputStream(dictionaryOutputFile.getAbsolutePath());
    for (int i = 0; i < intArray.length; i++) {
      out.writeInt(intArray[i]);
    }

    out.flush();
    out.close();
  }

  private void saveLongDictionary() throws IOException {
    long[] longArray = new long[longAVLTreeSet.size()];
    LongBidirectionalIterator iterator = longAVLTreeSet.iterator();
    for (int i = 0; i < longArray.length; i++) {
      longArray[i] = iterator.nextLong();
    }
    Arrays.sort(longArray);

    DataOutputStream out = InputOutputStreamUtils.getDefaultOutputStream(dictionaryOutputFile.getAbsolutePath());
    for (int i = 0; i < longArray.length; i++) {
      out.writeLong(longArray[i]);
    }

    out.flush();
    out.close();
  }

  private void saveFloatDictionary() throws IOException {
    float[] floatArray = new float[floatAVLTreeSet.size()];
    FloatBidirectionalIterator iterator = floatAVLTreeSet.iterator();
    for (int i = 0; i < floatArray.length; i++) {
      floatArray[i] = iterator.next();
    }

    Arrays.sort(floatArray);

    DataOutputStream out = InputOutputStreamUtils.getDefaultOutputStream(dictionaryOutputFile.getAbsolutePath());
    for (int i = 0; i < floatArray.length; i++) {
      out.writeFloat(floatArray[i]);
    }

    out.flush();
    out.close();
  }

  private void saveDoubleDictionary() throws IOException {
    double[] doubleArray = new double[doubleAVLTreeSet.size()];
    DoubleBidirectionalIterator iterator = doubleAVLTreeSet.iterator();
    for (int i = 0; i < doubleArray.length; i++) {
      doubleArray[i] = iterator.nextDouble();
    }

    Arrays.sort(doubleArray);

    DataOutputStream out = InputOutputStreamUtils.getDefaultOutputStream(dictionaryOutputFile.getAbsolutePath());
    for (int i = 0; i < doubleArray.length; i++) {
      out.writeDouble(doubleArray[i]);
    }

    out.flush();
    out.close();
  }

  public boolean isSorted() {
    return isSorted;
  }

  public boolean isMultiValued() {
    return isMultiValued;
  }

  public int getDictionarySize() {
    if (dictionary == null)
      return 0;
    return dictionary.size();
  }

  public int getTotalDocs() {
    return numDocsCounter;
  }

  public int getLengthOfEachEntry() {
    return longestStringLength;
  }

  public int indexOf(Object val) {
    return dictionary.indexOf(val);
  }

  public Dictionary<?> getDictionary() {
    return dictionary;
  }

  private void saveBooleanDictionary() throws IOException {
    saveStringDictionary();
  }

  private void saveStringDictionary() throws UnsupportedEncodingException, IOException {
    Iterator<String> iterator = stringSet.iterator();

    String[] arr = new String[stringSet.size()];
    int counter = 0;

    while (iterator.hasNext()) {
      StringBuilder val = new StringBuilder();
      String entry = iterator.next();
      for (int i = 0; i < (longestStringLength - entry.length()); i++) {
        val.append(V1Constants.Str.STRING_PAD_CHAR);
      }
      val.append(entry);
      arr[counter] = val.toString();
      counter++;
    }

    Arrays.sort(arr);
    DataOutputStream out = InputOutputStreamUtils.getDefaultOutputStream(dictionaryOutputFile.getAbsolutePath());
    for (int i = 0; i < arr.length; i++) {
      out.write(arr[i].getBytes(V1Constants.Str.CHAR_SET));
    }

    out.flush();
    out.close();
  }

  public int cardinality() {
    switch (spec.getDataType()) {
      case INT:
        return intAVLTreeSet.size();
      case LONG:
        return longAVLTreeSet.size();
      case FLOAT:
        return floatAVLTreeSet.size();
      case DOUBLE:
        return doubleAVLTreeSet.size();
      case BOOLEAN:
      case STRING:
        return stringSet.size();
      default:
        return 0;
    }
  }

  public Dictionary<?> seal() throws IOException {
    switch (spec.getDataType()) {
      case INT:
        saveIntDictionary();
        dictionary = new MmapIntDictionary(dictionaryOutputFile, intAVLTreeSet.size());
        break;
      case LONG:
        saveLongDictionary();
        dictionary = new MmapLongDictionary(dictionaryOutputFile, longAVLTreeSet.size());
        break;
      case FLOAT:
        saveFloatDictionary();
        return new MmapFloatDictionary(dictionaryOutputFile, floatAVLTreeSet.size());
      case DOUBLE:
        saveDoubleDictionary();
        dictionary = new MmapDoubleDictionary(dictionaryOutputFile, doubleAVLTreeSet.size());
        break;
      case BOOLEAN:
        saveBooleanDictionary();
        dictionary = new MmapBooleanDictionary(dictionaryOutputFile, stringSet.size(), longestStringLength);
        break;
      case STRING:
        saveStringDictionary();
        dictionary = new MmapStringDictionary(dictionaryOutputFile, stringSet.size(), longestStringLength);
        break;
      default:
        return null;
    }
    this.timeTaken = System.currentTimeMillis() - this.timeTaken;
    logger.info("persisted index for column : " + spec.getName() + " in " + dictionaryOutputFile.getAbsolutePath());
    return dictionary;
  }

  public long totalTimeTaken() {
    return this.timeTaken;
  }

  public void printStats() {
    System.out.println("name : " + spec.getName());
    System.out.println("type : " + spec.getDataType());
    System.out.println("numChanges : " + numberOfChanges);
    System.out.println("previousBigger : " + prevBiggerThanNextCount);
    System.out.println("isSorted : " + isSorted);
    System.out.println("maxLengthOfString : " + longestStringLength);

    StringBuilder b = new StringBuilder();
    IntBidirectionalIterator i = intAVLTreeSet.iterator();
    while (i.hasNext()) {
      b.append(i.nextInt() + ",");
    }
  }

  public static void main(String[] args) throws IOException {
    FieldSpec spec = new FieldSpec();
    spec.setDataType(DataType.INT);
    spec.setFieldType(FieldType.dimension);
    spec.setName("someField");
    spec.setSingleValueField(true);

    Random r = new Random(System.currentTimeMillis());
    DictionaryCreator c = new DictionaryCreator(spec, new File("/Users/dpatel/experiment/data/d"));

    for (int i = 0; i < 1000; i++) {
      c.add(new Integer(i));
    }

    Dictionary<?> d = c.seal();
    c.printStats();

    for (int i = 0; i < 1000; i++) {
      System.out.println(d.indexOf(new Integer(i)));
    }
  }

}
