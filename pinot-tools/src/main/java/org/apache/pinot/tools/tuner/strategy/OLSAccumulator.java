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
package org.apache.pinot.tools.tuner.strategy;

import io.vavr.Tuple2;
import java.util.ArrayList;
import java.util.HashMap;


/**
 * An implementation of {@link AbstractAccumulator}, to implement minimum pulling for {@link OLSAnalysisImpl}
 */
public class OLSAccumulator extends AbstractAccumulator {

  private ArrayList<Long> _timeList;
  private ArrayList<Long> _inFilterList;
  private HashMap<Tuple2<Long, Long>, Tuple2<Long, Long>> _minBin;

  public ArrayList<Long> getTimeList() {
    return _timeList;
  }

  public ArrayList<Long> getInFilterList() {
    return _inFilterList;
  }

  public HashMap<Tuple2<Long, Long>, Tuple2<Long, Long>> getMinBin() {
    return _minBin;
  }

  public OLSAccumulator() {
    _timeList = new ArrayList<>();
    _inFilterList = new ArrayList<>();
    _minBin = new HashMap<>();
  }

  public void merge(long time, long inFilter, long postFilter, long indexUsed, long binLen) {
    super.increaseCount();
    _timeList.add(time);
    _inFilterList.add(inFilter);
    Tuple2<Long, Long> key = new Tuple2<>(inFilter / binLen, postFilter / binLen);
    if (_minBin.containsKey(key)) {
      if (_minBin.get(key)._2() > time) {
        _minBin.put(key, new Tuple2<>(indexUsed, time));
      }
    } else {
      _minBin.put(new Tuple2<>(inFilter / binLen, postFilter / binLen), new Tuple2<>(indexUsed, time));
    }
  }

  public void merge(OLSAccumulator o2) {
    super.mergeCount(o2);
    _timeList.addAll(o2._timeList);
    _inFilterList.addAll(o2._inFilterList);
    o2._minBin.forEach((key, val) -> {
      if (_minBin.containsKey(key)) {
        if (_minBin.get(key)._2() > val._2()) {
          _minBin.put(key, val);
        }
      } else {
        _minBin.put(key, val);
      }
    });
  }

  @Override
  public String toString() {
    return "OLSMergerObj{" + "_timeList=" + _timeList + ", _inFilterList=" + _inFilterList + ", _minBin=" + _minBin + '}';
  }
}
