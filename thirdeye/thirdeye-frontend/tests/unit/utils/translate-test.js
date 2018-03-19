import translate from 'thirdeye-frontend/utils/translate';
import { module, test } from 'qunit';

module('Unit | Utility | translate', function() {
  test('translate function translates a value given a dictionary', function(assert) {
    const dictionary = {
      '1 hour': '1_HOURS',
      '1 day': '1_DAYS',
      '4 days': '4_DAYS'
    };

    assert.equal(translate(dictionary, '1_DAYS'), '1 day');
    assert.equal(translate(dictionary, '1_HOURS'), '1 hour');
    assert.equal(translate(dictionary, '4_DAYS'), '4 days');
    assert.equal(translate(dictionary, 'Not in the dictionary'), undefined);
  });
});
