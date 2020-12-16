import pubSub from 'thirdeye-frontend/utils/pub-sub';
import { module, test } from 'qunit';
import sinon from 'sinon';

module('Unit | Utility | pub sub');

test('it works', function (assert) {
  const testData = 'Test data';
  const onTestEventTrigger = sinon.fake();
  const subscription = pubSub.subscribe('testEvent', onTestEventTrigger);

  pubSub.publish('testEvent', testData);
  assert.ok(onTestEventTrigger.calledOnceWithExactly(testData));

  subscription.unSubscribe();
  sinon.reset();
  pubSub.publish('testEvent', testData);
  assert.notOk(onTestEventTrigger.called);
});
