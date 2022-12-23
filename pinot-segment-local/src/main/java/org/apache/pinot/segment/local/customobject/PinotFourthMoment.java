/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.customobject;

import java.nio.ByteBuffer;
import java.util.Comparator;
import org.apache.commons.math.stat.descriptive.moment.FourthMoment;
import org.apache.commons.math.stat.descriptive.moment.Kurtosis;
import org.apache.commons.math.stat.descriptive.moment.Skewness;


/**
 * A {@link Comparable} implementation of the <a href=https://en.wikipedia.org/wiki/Moment_(mathematics)>
 * Fourth Statistical Moment</a> that uses the apache commons algorithm for computing it in
 * one pass. It additionally supports serialization and deserialization methods, which is helpful
 * for combining moments across servers.
 *
 * <p>The commons implementation does not support parallel-computation, support for which is added
 * in the {@link #combine(PinotFourthMoment)} method inspired by Presto's implementation.
 *
 * <pre>
 * Also See: <a href="https://github.com/prestodb/presto/blob/master/presto-main/src/main/java/com/facebook
 * /presto/operator/aggregation/AggregationUtils.java#L188">Presto's Implementation</a>
 * </pre>
 */
public class PinotFourthMoment extends FourthMoment implements Comparable<PinotFourthMoment> {

  private static final Comparator<PinotFourthMoment> COMPARATOR = Comparator.<PinotFourthMoment>naturalOrder()
      .thenComparingLong(x -> x.n)
      .thenComparingDouble(x -> x.m1)
      .thenComparingDouble(x -> x.m2)
      .thenComparingDouble(x -> x.m3)
      .thenComparingDouble(x -> x.m4);

  public void combine(PinotFourthMoment other) {
    combine(other.n, other.m1, other.m2, other.m3, other.m4);
  }

  public void combine(long bN, double bM1, double bM2, double bM3, double bM4) {
    if (bN == 0) {
      return;
    } else if (n == 0) {
      n = bN;
      m1 = bM1;
      m2 = bM2;
      m3 = bM3;
      m4 = bM4;
      return;
    }

    long aN = n;
    double aM1 = m1;
    double aM2 = m2;
    double aM3 = m3;
    double aM4 = m4;

    long n = aN + bN;
    double m1 = (aN * aM1 + bN * bM1) / n;

    double delta = bM1 - aM1;
    double delta2 = delta * delta;
    double m2 = aM2 + bM2 + delta2 * aN * bN / n;

    double delta3 = delta2 * delta;
    double m3 = aM3 + bM3
        + delta3 * aN * bN * (aN - bN) / (n * n)
        + 3d * delta * (aN * bM2 - bN * aM2) / n;

    double delta4 = delta3 * delta;
    double n3 = ((double) n) * n * n; // avoid overflow
    double m4 = aM4 + bM4
        + delta4 * aN * bN * (aN * aN - aN * bN + bN * bN) / (n3)
        + 6.0 * delta2 * (aN * aN * bM2 + bN * bN * aM2) / (n * n)
        + 4d * delta * (aN * bM3 - bN * aM3) / n;

    this.n = n;
    this.m1 = m1;
    this.m2 = m2;
    this.m3 = m3;
    this.m4 = m4;
  }

  public double skew() {
    return new Skewness(this).getResult();
  }

  public double kurtosis() {
    return new Kurtosis(this).getResult();
  }

  public byte[] serialize() {
    ByteBuffer buff = ByteBuffer.allocate(Long.BYTES + Double.BYTES * 4);
    buff.putLong(n)
        .putDouble(m1)
        .putDouble(m2)
        .putDouble(m3)
        .putDouble(m4);
    return buff.array();
  }

  public static PinotFourthMoment fromBytes(byte[] bytes) {
    return fromBytes(ByteBuffer.wrap(bytes));
  }

  public static PinotFourthMoment fromBytes(ByteBuffer buff) {
    PinotFourthMoment moment = new PinotFourthMoment();
    moment.n = buff.getLong();
    moment.m1 = buff.getDouble();
    moment.m2 = buff.getDouble();
    moment.m3 = buff.getDouble();
    moment.m4 = buff.getDouble();
    return moment;
  }

  @Override
  public int compareTo(PinotFourthMoment o) {
    return COMPARATOR.compare(this, o);
  }
}
