package org.apache.pinot.tools.tuner.strategy;

import io.vavr.Tuple2;
import java.util.ArrayList;
import java.util.HashMap;


public class OLSMergerObj extends BasicMergerObj {

  ArrayList<Long> _timeList = null;
  ArrayList<Long> _inFilterList = null;
  HashMap<Tuple2<Long, Long>, Tuple2<Long, Long>> _minBin = null;

  public ArrayList<Long> getTimeList() {
    return _timeList;
  }

  public ArrayList<Long> getInFilterList() {
    return _inFilterList;
  }

  public HashMap<Tuple2<Long, Long>, Tuple2<Long, Long>> getMinBin() {
    return _minBin;
  }

  public OLSMergerObj() {
    _timeList = new ArrayList<>();
    _inFilterList = new ArrayList<>();
    _minBin = new HashMap<>();
  }

  public void merge(long time, long inFilter, long postFilter, long indexUsed, long binLen) {
    super.addCount();
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

  public void merge(OLSMergerObj o2) {
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
    return "OLSMergerObj{" + "_timeList=" + _timeList + ", _inFilterList=" + _inFilterList + ", _minBin=" + _minBin
        + '}';
  }
}
