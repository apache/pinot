import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { optionsToString } from 'thirdeye-frontend/tests/utils/constants';

module('Integration | Component | self serve alert details', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    this.set('alerts', [server.createList('alert', 1)[0].attrs][0]);
    const actualPropNames = ["Metric", "Dataset", "Granularity", "Application", "Alert Owner", "Data Filter", "Dimensions", "Detection Type", "Subscription Group"];
    const headerSel = '.te-search-results__header .te-search-results__tag';
    const propTitleSel = '.te-search-results__row .te-search-results__option';
    const titleSel = '.te-search-results__title span';

    await render(hbs`
      <div class="container">
        {{#self-serve-alert-details
          alertData=alerts
          isLoadError=false
          displayMode="single"
        }}
          template block text
        {{/self-serve-alert-details}}
      </div>
    `);

    // Collect all rendered alert prop names
    const renderedPropNames = optionsToString(this.$(propTitleSel));

    assert.equal(
      this.$(headerSel).get(0).innerText.trim(),
      'ACTIVE',
      'correctly displays alert status'
    );

    assert.equal(
      this.$(titleSel).get(0).innerText.trim(),
      'test_function_1',
      'correctly displays alert title'
    );

    assert.ok(
      actualPropNames.every(name => renderedPropNames.includes(name)),
      'correctly displays property names'
    );
  });

});
