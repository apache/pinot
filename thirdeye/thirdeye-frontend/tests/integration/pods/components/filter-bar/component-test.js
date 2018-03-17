import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import config from 'thirdeye-frontend/shared/filterBarConfig';

module('Integration | Component | filter bar', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders well', async function(assert) {
    const headers = config.map((category) => category.header);
    this.setProperties({
      entities: {},
      config,
      onFilterSelection: () => {}
    });

    await render(hbs`
      {{filter-bar
        entities=entities
        onFilter=(action onFilterSelection)
        config=config
      }}
    `);

    const filterBarText = this.$().text().trim();
    headers.forEach((header) => {
      assert.ok(filterBarText.includes(header), `filter bar should display ${header}`);
    });
  });
});
