package org.apache.pinot.fsa;

import org.apache.pinot.fsa.builders.FSA5Serializer;


/**
 * 
 */
public class FSA5SerializerTest extends SerializerTestBase {
  protected FSA5Serializer createSerializer() {
    return new FSA5Serializer();
  }
}
