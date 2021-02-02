import { module, test } from 'qunit';
import { isObject, checkForMatch } from 'thirdeye-frontend/utils/utils';

module('Unit | Utility | Basic utils tests', function () {
  test('it determines correctly if the data type is of the type object', function (assert) {
    assert.notOk(isObject('hello'));
    assert.notOk(isObject(4));
    assert.notOk(isObject(true));
    assert.notOk(isObject(null));
    assert.notOk(isObject(['hi', 1]));
    assert.notOk(isObject(() => {}));

    assert.ok(isObject({ prop1: 1, prop2: 'hi' }));
  });

  test('it detects the substring match in the input correctly', function (assert) {
    //String input
    let input = 'hello';
    assert.ok(checkForMatch(input, 'hel'));
    assert.notOk(checkForMatch(input, 'hef'));

    //Boolean input
    input = true;
    assert.ok(checkForMatch(input, 'tru'));
    assert.notOk(checkForMatch(input, 's'));

    //Numeric input
    input = 12;
    assert.ok(checkForMatch(12, '1'));
    assert.ok(checkForMatch(12, '12'));
    assert.notOk(checkForMatch(12, '13'));

    //Array input - Substring match in any one of the entries should return true
    input = ['hi', 'hello'];
    assert.ok(checkForMatch(input, 'hel'));
    assert.notOk(checkForMatch(input, 'test'));

    //Object input - Substring match in any one of the properties or any one of the property values should return true
    input = { propOne: 124, propTwo: 3 };
    assert.ok(checkForMatch(input, 'One'));
    assert.ok(checkForMatch(input, '12'));
    assert.notOk(checkForMatch(input, 'three'));
  });
});
