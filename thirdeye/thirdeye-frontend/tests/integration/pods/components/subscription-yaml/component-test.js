/**
 * Integration tests for the subscription-yaml component.
 * @property {Array} subscriptionGroupNames - an array of objects containing subscription group information
 * @property {boolean} isEditMode - to activate the edit mode
 * @example
 {{subscription-yaml
   isEditMode=true
   subscriptionYaml=subscriptionYaml
   subscriptionMsg=""
   subscriptionGroupNamesDisplay=subscriptionGroupNamesDisplay
   groupName=groupName
   createGroup=createGroup
   }}
 * @author hjackson
 */

import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | subscription-yaml', function(hooks) {
  setupRenderingTest(hooks);
  const testText = 'default yaml';

  test(`displays yaml of subscription group in edit mode`, async function(assert) {
    this.setProperties({
      subscriptionGroups: [],
      subscriptionYaml: testText
    });

    await render(hbs`
      {{subscription-yaml
        isEditMode=true
        subscriptionYaml=subscriptionYaml
        subscriptionMsg=""
        subscriptionGroupNamesDisplay=subscriptionGroupNamesDisplay
        groupName=groupName
        createGroup=createGroup
      }}
    `);
    assert.ok(this.$('.ace_line')[0].innerText === testText);
  });

  test(`displays default subscription group yaml in create mode`, async function(assert) {

    const defaultText = "# subscriptionGroupName: 'give_a_unique_name_to_this_group'";
    await render(hbs`
      {{subscription-yaml
        isEditMode=false
        showSettings=true
      }}
    `);

    assert.ok(this.$('.ace_line')[0].children[0].textContent === defaultText);
  });
});
