import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | shared/common-tabs', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders component `shared/common-tabs` well', async function(assert) {
    this.setProperties({
      activeTab: 'metrics'
    });

    await render(hbs`
      {{#shared/common-tabs selection=activeTab activeTab=activeTab as |tabs|}}
        {{#tabs.tablist as |tablist|}}
          {{#tablist.tab name="metrics"}}Metrics{{/tablist.tab}}
          {{#tablist.tab name="dimensions"}}Dimensions{{/tablist.tab}}
        {{/tabs.tablist}}

        {{!-- metrics --}}
        {{#tabs.tabpanel name="metrics"}}
          {{!-- IMPORTANT metrics CODE --}}
        {{/tabs.tabpanel}}
        {{!-- dimensions --}}
        {{#tabs.tabpanel name="dimensions"}}
          {{!-- IMPORTANT dimensions CODE --}}
        {{/tabs.tabpanel}}
      {{/shared/common-tabs}}
    `);

    const $tabs = this.$('.common-tabs ul li');

    // Testing tabs
    assert.equal($tabs.get(0).innerText.trim(), 'Metrics', 'Name of the first tab matches `Metrics` is correct.');
    assert.equal($tabs.get(1).innerText.trim(), 'Dimensions', 'Name of the first tab matches `Dimensions` is correct.');
  });
});
