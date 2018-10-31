import $ from 'jquery';
import { module, test } from 'qunit';
import { setupApplicationTest } from 'ember-qunit';
import { selfServeConst } from 'thirdeye-frontend/tests/utils/constants';
import { visit, fillIn, click, currentURL, triggerKeyEvent } from '@ember/test-helpers';

module('Acceptance | import metric', function(hooks) {
  setupApplicationTest(hooks);

  const importLinkText = 'Import a Metric From InGraphs';
  const returnLinkText = 'Back to Create';
  const inGraphsLinkText = 'Go to InGraphs';
  const dashboardToImport = 'thirdeye-all';
  const importWarning = 'Warning... This dashboard name cannot be found in inGraphs. Please verify.';
  const importSuccessMsgTitle = 'Success! Metrics On-boarded';
  const importSuccessMsgText1 = `You have successfully onboarded the ${dashboardToImport} dataset , including the following metrics`;

  test(`visiting alert creation page to navigate to import page`, async (assert) => {
    await visit(`/self-serve/create-alert`);

    assert.equal(
      $(selfServeConst.SECONDARY_LINK).get(0).innerText.trim(),
      importLinkText,
      'Import link appears.'
    );

    await click(selfServeConst.SECONDARY_LINK);

    assert.equal(
      currentURL(),
      '/self-serve/import-metric',
      'Navigation to import page succeeded');
    assert.ok(
      $(`${selfServeConst.SECONDARY_LINK}:contains(${returnLinkText})`).get(0),
      'Return link appears.'
    );
    assert.ok(
      $(`${selfServeConst.SECONDARY_LINK}:contains(${inGraphsLinkText})`).get(0),
      'InGraph link appears.'
    );
  });

  test(`visiting metric import page to import existing dashboard`, async (assert) => {
    await visit(`/self-serve/import-metric`);

    const $dashboardInput = $(selfServeConst.IMPORT_DASHBOARD_INPUT);
    let $submitButton = $(selfServeConst.SUBMIT_BUTTON);

    assert.ok(
      $submitButton.get(0).disabled,
      'Submit button is still disabled'
    );
    assert.ok(
      $(selfServeConst.IMPORT_DASHBOARD_INPUT).length,
      'Import dashboard field renders.'
    );

    // Filling in a non-existent dashboard name
    await fillIn(selfServeConst.IMPORT_DASHBOARD_INPUT, 'test123X');

    assert.notOk(
      $submitButton.get(0).disabled,
      'Submit button is enabled'
    );

    // Submitting a non-existent dashboard name
    await click(selfServeConst.SUBMIT_BUTTON);

    assert.equal(
      $(selfServeConst.IMPORT_WARNING).get(0).innerText.trim(),
      importWarning,
      'Non-existent dashboard name results in warning'
    );
    assert.ok(
      $submitButton.get(0).disabled,
      'Submit button is now disabled.'
    );

    // Filling in an existing dashboard name
    await fillIn(selfServeConst.IMPORT_DASHBOARD_INPUT, dashboardToImport);
    // Trigger key event in field to get submit button to detect change
    await triggerKeyEvent(selfServeConst.IMPORT_DASHBOARD_INPUT, 'keyup', '8');

    assert.notOk(
      $submitButton.get(0).disabled,
      'Submit button is enabled'
    );

    // Submit new dashboard name
    await click(selfServeConst.SUBMIT_BUTTON);

    assert.ok(
      $(selfServeConst.IMPORT_SUCCESS).get(0).innerText.trim().includes(importSuccessMsgText1),
      'Import dashboard successful - success text displays)'
    );
    assert.ok(
      $(selfServeConst.IMPORT_RESULT_LIST).children().length === 5,
      'At least one metric was imported'
    );
    assert.equal(
      $(selfServeConst.SUBMIT_BUTTON).get(0).innerText.trim(),
      'Onboard Another Dashboard',
      'Submit button mode changed correctly'
    );
    assert.ok(
      $dashboardInput.get(0).disabled,
      'Custom fields are disabled'
    );

    // Click button to 'onboard another dashboard'
    await click(selfServeConst.SUBMIT_BUTTON);

    assert.equal(
      $(selfServeConst.SUBMIT_BUTTON).get(0).innerText.trim(),
      'Import Metrics',
      'Submit button mode changed correctly'
    );
    assert.notOk(
      $dashboardInput.get(0).disabled,
      'Custom fields are now enabled again.'
    );
  });

  test(`visiting metric import page to import custom dashboard (create new metrics via rrd import)`, async (assert) => {
    await visit(`/self-serve/import-metric`);

    const rrdName = 'my-new-rrd';
    const metricName = 'my-new-metric';
    const datasetName = 'my-new-dataset';
    const importSuccessMsgText2 = `You have successfully onboarded the ${datasetName} dataset , including the following metrics`;
    let $submitButton = $(selfServeConst.SUBMIT_BUTTON);

    // Provide new dataset name
    await fillIn(selfServeConst.IMPORT_DATASET_INPUT, datasetName);

    assert.ok(
      $submitButton.get(0).disabled,
      'Submit button is disabled.'
    );
    assert.ok(
      $(selfServeConst.IMPORT_DASHBOARD_INPUT).get(0).disabled,
      'Import existing dashboard by name field is disabled.'
    );

    // Provide new metric name
    await fillIn(selfServeConst.IMPORT_METRIC_INPUT, metricName);

    assert.ok(
      $submitButton.get(0).disabled,
      'Submit button is still disabled.'
    );

    // Provide existin RRD key
    await fillIn(selfServeConst.IMPORT_RRD_INPUT, rrdName);

    assert.notOk(
      $submitButton.get(0).disabled,
      'Submit button is now enabled'
    );

    // Submit for successful response
    await click(selfServeConst.SUBMIT_BUTTON);

    assert.ok(
      $(selfServeConst.IMPORT_SUCCESS).get(0).innerText.trim().includes(importSuccessMsgText2),
      'Import dashboard successful - success text displays)'
    );
    assert.ok(
      $(selfServeConst.IMPORT_RESULT_LIST).children().length === 5,
      'At least one metric was imported'
    );
    assert.equal(
      $(selfServeConst.SUBMIT_BUTTON).get(0).innerText.trim(),
      'Onboard Another Dashboard',
      'Submit button mode changed correctly'
    );
  });
});
