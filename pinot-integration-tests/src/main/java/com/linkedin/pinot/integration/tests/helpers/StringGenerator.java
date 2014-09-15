package com.linkedin.pinot.integration.tests.helpers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang.RandomStringUtils;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 12, 2014
 */

public class StringGenerator implements Generator {

  private static final int lengthOfEachString = 10;

  private final int cardinality;
  private final Random rand;
  private List<String> vals;

  public StringGenerator(Integer cardinality) {
    this.cardinality = cardinality.intValue();
    rand = new Random(System.currentTimeMillis());
  }

  @Override
  public void init() {
    final Set<String> uniqueStrings = new HashSet<String>();
    for (int i = 0; i < cardinality; i++) {
      while (!uniqueStrings.add(RandomStringUtils.randomAlphabetic(lengthOfEachString))) {
        uniqueStrings.add(RandomStringUtils.randomAlphabetic(lengthOfEachString));
      }
    }
    vals = new ArrayList<String>(uniqueStrings);
  }

  @Override
  public Object next() {
    return vals.get(rand.nextInt(cardinality));
  }

  public static void main(String[] args) {
    final StringGenerator gen = new StringGenerator(10000);
    gen.init();
    for (int i = 0; i < 1000000; i++) {
      System.out.println(gen.next());
    }
  }
}
