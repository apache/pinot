package com.linkedin.pinot.segments.v1.creator;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.Random;
import java.util.TreeSet;

import com.linkedin.pinot.index.data.FieldSpec;
import com.linkedin.pinot.index.data.FieldSpec.DataType;
import com.linkedin.pinot.index.data.FieldSpec.FieldType;
import com.linkedin.pinot.segments.utils.InputOutputStreamUtils;
import com.linkedin.pinot.segments.v1.creator.V1Constants.Dict;
import com.linkedin.pinot.segments.v1.segment.dictionary.BooleanDictionary;
import com.linkedin.pinot.segments.v1.segment.dictionary.Dictionary;
import com.linkedin.pinot.segments.v1.segment.dictionary.DoubleDictionary;
import com.linkedin.pinot.segments.v1.segment.dictionary.FloatDictionary;
import com.linkedin.pinot.segments.v1.segment.dictionary.IntDictionary;
import com.linkedin.pinot.segments.v1.segment.dictionary.LongDictionary;
import com.linkedin.pinot.segments.v1.segment.dictionary.StringDictionary;

import it.unimi.dsi.fastutil.doubles.Double2IntMap;
import it.unimi.dsi.fastutil.doubles.DoubleAVLTreeSet;
import it.unimi.dsi.fastutil.doubles.DoubleBidirectionalIterator;
import it.unimi.dsi.fastutil.floats.Float2IntOpenHashMap;
import it.unimi.dsi.fastutil.floats.FloatAVLTreeSet;
import it.unimi.dsi.fastutil.floats.FloatBidirectionalIterator;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntBidirectionalIterator;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.LongAVLTreeSet;
import it.unimi.dsi.fastutil.longs.LongBidirectionalIterator;
import it.unimi.dsi.fastutil.objects.Object2IntMap;

/**
 * Jun 30, 2014
 * 
 * @author Dhaval Patel <dpatel@linkedin.com>
 * @param <E>
 * 
 */

public class DictionaryCreator {

  // persistence information
  private File indexDir;
  private File dictionaryOutputFile;

  // typed tree setss

  private IntAVLTreeSet intAVLTreeSet;
  private FloatAVLTreeSet floatAVLTreeSet;
  private LongAVLTreeSet longAVLTreeSet;
  private DoubleAVLTreeSet doubleAVLTreeSet;
  private TreeSet<String> stringSet;

  // string padding length helper
  private int longestStringLength = 0;

  // stats collectors
  private int numDocsCounter;
  private Object previousValue = null;
  private boolean isSorted = true;
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
    dictionaryOutputFile = new File(indexDir, spec.getName() + ".dict");
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
        value = (String)value;
      if (value instanceof Boolean)
        value = Boolean.valueOf((Boolean)value);
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

  private void saveIntDictionary() throws IOException {
    DataOutputStream out = InputOutputStreamUtils
        .getDefaultOutputStream(dictionaryOutputFile.getAbsolutePath());
    IntBidirectionalIterator iterator = intAVLTreeSet.iterator();
    while (iterator.hasNext()) {
      out.writeInt(iterator.next());
    }
    out.flush();
    out.close();
  }

  private void saveLongDictionary() throws IOException {
    DataOutputStream out = InputOutputStreamUtils
        .getDefaultOutputStream(dictionaryOutputFile.getAbsolutePath());
    LongBidirectionalIterator iterator = longAVLTreeSet.iterator();
    while (iterator.hasNext()) {
      out.writeLong(iterator.next());
    }
    out.flush();
    out.close();
  }

  private void saveFloatDictionary() throws IOException {
    DataOutputStream out = InputOutputStreamUtils
        .getDefaultOutputStream(dictionaryOutputFile.getAbsolutePath());
    FloatBidirectionalIterator iterator = floatAVLTreeSet.iterator();
    while (iterator.hasNext()) {
      out.writeFloat(iterator.next());
    }
    out.flush();
    out.close();
  }

  private void saveDoubleDictionary() throws IOException {
    DataOutputStream out = InputOutputStreamUtils
        .getDefaultOutputStream(dictionaryOutputFile.getAbsolutePath());
    DoubleBidirectionalIterator iterator = doubleAVLTreeSet.iterator();
    while (iterator.hasNext()) {
      out.writeDouble(iterator.next());
    }
    out.flush();
    out.close();
  }

  private void saveBooleanDictionary() throws IOException {
    saveStringDictionary();
  }

  private void saveStringDictionary() throws UnsupportedEncodingException,
      IOException {
    Iterator<String> iterator = stringSet.iterator();
    TreeSet<String> sortedAfterPadding = new TreeSet<String>();

    while (iterator.hasNext()) {
      StringBuilder val = new StringBuilder();
      String entry = iterator.next();
      for (int i = 0; i < (longestStringLength - entry.length()); i++) {
        val.append(V1Constants.Str.STRING_PAD_CHAR);
      }
      val.append(entry);
      sortedAfterPadding.add(val.toString());
    }

    DataOutputStream out = InputOutputStreamUtils
        .getDefaultOutputStream(dictionaryOutputFile.getAbsolutePath());
    Iterator<String> it = sortedAfterPadding.iterator();
    while (it.hasNext()) {
      out.write(it.next().toString().getBytes(V1Constants.Str.CHAR_SET));
    }

    out.flush();
    out.close();
  }

  public Dictionary<?> seal() throws IOException {
    switch (spec.getDataType()) {
    case INT:
      saveIntDictionary();
      return new IntDictionary(dictionaryOutputFile, intAVLTreeSet.size());
    case LONG:
      saveLongDictionary();
      return new LongDictionary(dictionaryOutputFile, longAVLTreeSet.size());
    case FLOAT:
      saveFloatDictionary();
      return new FloatDictionary(dictionaryOutputFile, floatAVLTreeSet.size());
    case DOUBLE:
      saveDoubleDictionary();
      return new DoubleDictionary(dictionaryOutputFile, doubleAVLTreeSet.size());
    case BOOLEAN:
      saveBooleanDictionary();
      return new BooleanDictionary(dictionaryOutputFile, stringSet.size(),
          longestStringLength);
    case STRING:
      saveStringDictionary();
      return new StringDictionary(dictionaryOutputFile, stringSet.size(),
          longestStringLength);
    default:
      return null;
    }
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
    DictionaryCreator c = new DictionaryCreator(spec, new File(
        "/Users/dpatel/experiment/data/d"));

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
