package org.apache.pinot.fsa;

import org.apache.pinot.fsa.builders.CFSA2Serializer;


/**
 * 
 */
public class CFSA2SerializerTest extends SerializerTestBase {
  protected CFSA2Serializer createSerializer() {
    return new CFSA2Serializer();
  }
}
