package org.apache.pinot.core.operator.dociditerators;

import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.segment.spi.Constants;


public class NotDocIdIterator implements BlockDocIdIterator {
  private BlockDocIdIterator _childDocIdIterator;
  private int _currentDocIdFromChildIterator;
  private int _lowerLimit;
  private int _upperLimit;
  private int _numDocs;

  public NotDocIdIterator(BlockDocIdIterator childDocIdIterator, int numDocs) {
    _childDocIdIterator = childDocIdIterator;
    _currentDocIdFromChildIterator = childDocIdIterator.next();
    _lowerLimit = 0;
    _upperLimit = _currentDocIdFromChildIterator == Constants.EOF ? numDocs : _currentDocIdFromChildIterator;
    _numDocs = numDocs;
  }

  @Override
  public int next() {
    if (_lowerLimit == _upperLimit) {
      if (_lowerLimit == _numDocs) {
        return Constants.EOF;
      }
      _lowerLimit = _upperLimit + 1;

      int nextMatchingDocId = _childDocIdIterator.next();

      if (nextMatchingDocId == Constants.EOF) {
        _upperLimit = _numDocs;
      } else {
        _upperLimit = nextMatchingDocId;
      }
    }

    return _lowerLimit++;
  }

  @Override
  public int advance(int targetDocId) {
    if (targetDocId == Constants.EOF) {
      return targetDocId;
    }

    if (targetDocId < _lowerLimit) {
      return _lowerLimit;
    }

    _lowerLimit = targetDocId + 1;
    //int upperLimit = _childDocIdIterator.advance(targetDocId);
    int upperLimit = findUpperLimitGreaterThanDocId(targetDocId);

    if (upperLimit == Constants.EOF) {
      _upperLimit = _numDocs;
    } else {
      _upperLimit = upperLimit;
      _currentDocIdFromChildIterator = _upperLimit;
    }

    return _lowerLimit++;
  }

  private int findUpperLimitGreaterThanDocId(int currentDocId) {
    int result = _childDocIdIterator.advance(currentDocId);

    while (result <= currentDocId && result != Constants.EOF) {
      result = _childDocIdIterator.next();
    }

    return result;
  }
}
