import { module, test } from 'qunit';
import { setupApplicationTest } from 'ember-qunit';
import { visit, fillIn, click, currentURL } from '@ember/test-helpers';
import { selfServeConst } from 'thirdeye-frontend/tests/utils/constants';
import $ from 'jquery';

module('Acceptance | edit alert', function(hooks) {
  setupApplicationTest(hooks);

  test(`visiting ${selfServeConst.EDIT_LINK} and checking that fields render correctly and edit is successful`, async (assert) => {
    const alert = server.create('alert');
    const editSuccessMsg = `Edit Alert Success! You have successfully edited alert ${alert.id}`;
    const editNotificationButtonText = 'Edit Notification Settings';

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
    assert.ok(
      $(`button:contains(${editNotificationButtonText})`).get(0),
      'Subscription group button renders ok');

/*  TODO: Test completion needed for new notification settings modal
    await fillIn(selfServeConst.ALERT_NAME_INPUT, selfServeConst.NEW_FUNC_NAME);
    await click(selfServeConst.STATUS_TOGGLER);
    await click(selfServeConst.SUBMIT_BUTTON);
    assert.ok(
      $(selfServeConst.IMPORT_SUCCESS).get(0).innerText.includes(editSuccessMsg),
      'after edit, alert is saved successfully');
*/
  });
});
