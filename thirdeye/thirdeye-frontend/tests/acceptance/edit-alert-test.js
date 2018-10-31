import { module, test } from 'qunit';
import { setupApplicationTest } from 'ember-qunit';
import { visit, fillIn, click, currentURL } from '@ember/test-helpers';
import { selfServeConst } from 'thirdeye-frontend/tests/utils/constants';
import $ from 'jquery';

module('Acceptance | edit alert', function(hooks) {
  setupApplicationTest(hooks);

  test(`visiting ${selfServeConst.EDIT_LINK} and checking that fields render correctly and edit is successful`, async (assert) => {
    const alert = server.create('alert');
    await visit(`/manage/alert/${alert.id}/edit`);

    assert.equal(
      currentURL(),
      `/manage/alert/1/edit`,
      'correctly redirects to edit alerts page'
    );
    assert.equal(
      $(selfServeConst.ALERT_NAME_INPUT).get(0).value,
      'test_function_1',
      'alert name is correct');
    assert.equal(
      $(selfServeConst.SUBSCRIPTION_GROUP).get(0).innerText,
      'beauty-and-the-beast',
      'subscription group name is correct');
    assert.equal(
      $(selfServeConst.STATUS).get(0).innerText,
      'Active',
      'alert status is correct');

    await fillIn(selfServeConst.ALERT_NAME_INPUT, selfServeConst.NEW_FUNC_NAME);
    await click(selfServeConst.STATUS_TOGGLER);
    await click(selfServeConst.SUBMIT_BUTTON);

    assert.equal(
      currentURL(),
      '/manage/alerts',
      'correctly redirects to manage alerts page after edit'
    );

    assert.equal(
      $(`${selfServeConst.NEW_FUNC_RESULT}[title='${selfServeConst.NEW_FUNC_NAME}']`).get(0).innerText,
      selfServeConst.NEW_FUNC_NAME,
      'after edit, alert name is saved correctly');
    assert.equal(
      $(selfServeConst.STATUS_RESULT).get(0).innerText,
      'Inactive',
      'after edit, alert status is saved correctly');
  });
});
