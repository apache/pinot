/**
 * Integration tests for the yaml-editor component.
 * @property {number} alertId - the alert id
 * @property {Array} subscriptionGroupNames - an array of objects containing subscription group information
 * @property {boolean} isEditMode - to activate the edit mode
 * @property {boolean} showSettings - to show the subscriber groups yaml editor
 * @property {string} detectionYaml - the detection yaml to display
 * @example
   {{yaml-editor
     alertId=1
     subscriptionGroupId=1
     isEditMode=true
     showSettings=true
     subscriptionGroupNames=subscriptionGroupNames
     detectionYaml=detectionYaml
   }}
 * @author hjackson
 */

import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | yaml-editor', function(hooks) {
  setupRenderingTest(hooks);
  const testText = 'default yaml';

  test(`displays yaml file of alert configuration in edit mode`, async function(assert) {
    this.setProperties({
      alertId: 1,
      subscriptionGroups: [],
      detectionYaml: testText
    });

    await render(hbs`
      {{yaml-editor
        alertId=alertId
        isEditMode=true
        showSettings=true
        subscriptionGroupNames=subscriptionGroups
        detectionYaml=detectionYaml
      }}
    `);
    assert.ok(this.$('.ace_line')[0].innerText === testText);
  });

  test(`displays default yaml file of alert configuration in create mode`, async function(assert) {

    const defaultText = '# Below is a sample template for setting up a WoW percentage rule. You may refer the documentation for more examples and update the fields accordingly.';
    await render(hbs`
      {{yaml-editor
        isEditMode=false
        showSettings=true
      }}
    `);

    assert.ok(this.$('.ace_line')[0].children[0].textContent === defaultText);
  });

  test(`displays first subscription group in dropdown in edit mode`, async function(assert) {
    this.setProperties({
      alertId: 1,
      subscriptionGroups: [ {
        id : 1,
        name : 'test_subscription_group',
        yaml : testText
      } ],
      detectionYaml: testText
    });

    await render(hbs`
      {{yaml-editor
        alertId=alertId
        isEditMode=true
        showSettings=true
        subscriptionGroupNames=subscriptionGroups
        detectionYaml=detectionYaml
      }}
    `);

    assert.ok(this.$('.ember-power-select-selected-item')[0].innerText === `test_subscription_group (1)`);
  });

  test(`displays first subscription group yaml in edit mode`, async function(assert) {
    this.setProperties({
      alertId: 1,
      subscriptionGroups: [ {
        id : 1,
        name : 'test_subscription_group',
        yaml : testText
      } ],
      detectionYaml: 'nothing'
    });

    await render(hbs`
      {{yaml-editor
        alertId=alertId
        isEditMode=true
        showSettings=true
        subscriptionGroupNames=subscriptionGroups
        detectionYaml=detectionYaml
      }}
    `);

    assert.ok(this.$('.ace_line')[1].innerText === testText);
  });

  test(`displays default yaml file of subscription group in create mode`, async function(assert) {

    const defaultText = '# Below is a sample subscription group template. You may refer the documentation and update accordingly.';
    await render(hbs`
      {{yaml-editor
        isEditMode=false
        showSettings=true
      }}
    `);

    assert.ok(this.$('.ace_line')[29].children[0].textContent === defaultText);
  });
});
