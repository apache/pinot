import { test } from 'qunit';
import moduleForAcceptance from 'thirdeye-frontend/tests/helpers/module-for-acceptance';

const PLACEHOLDER = '.rootcause-placeholder';
const TABS = '.rootcause-tabs';

moduleForAcceptance('Acceptance | rootcause');

test('empty state of rootcause page should have a placeholder and no tabs', async (assert) => {
  await visit('/rootcause');

  assert.equal(
    currentURL(),
    '/rootcause',
    'link is correct');
  assert.ok(
    find(PLACEHOLDER).get(0),
    'placeholder exists'
  );
  assert.notOk(
    find(TABS).get(0),
    'tabs do not exist'
  );
});
