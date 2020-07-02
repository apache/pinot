/**
 * Integration tests for the detection-yaml component.
 * @property {number} alertId - the alert id
 * @property {Array} subscriptionGroupNames - an array of objects containing subscription group information
 * @property {boolean} isEditMode - to activate the edit mode
 * @property {boolean} showSettings - to show the subscriber groups yaml editor
 * @property {string} detectionYaml - the detection yaml to display
 * @example
   {{detection-yaml
     alertId=1
     isEditMode=true
     detectionYaml=detectionYaml
   }}
 * @author hjackson
 */

import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | detection-yaml', function(hooks) {
  setupRenderingTest(hooks);
  const testText = 'default yaml';

  test(`displays yaml file of detection configuration in edit mode`, async function(assert) {
    this.setProperties({
      alertId: 1,
      subscriptionGroups: [],
      detectionYaml: testText
    });

    await render(hbs`
      {{detection-yaml
        isEditMode=true
        alertId=alertId
        detectionYaml=detectionYaml
      }}
    `);
    assert.ok(this.$('.ace_line')[0].innerText === testText);
  });

  test(`displays default detection yaml in create mode`, async function(assert) {

    const defaultText = "detectionName: 'give_a_unique_name_to_this_alert'";
    await render(hbs`
      {{detection-yaml
        isEditMode=false
      }}
    `);

    assert.ok(this.$('.ace_line')[0].innerText === defaultText);
  });
});
