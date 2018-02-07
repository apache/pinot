import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import config from 'thirdeye-frontend/mocks/filterBarConfig';

moduleForComponent('filter-bar', 'Integration | Component | filter bar', {
  integration: true
});

test('it renders well', function(assert) {
  const headers = config.map((category) => category.header);
  this.setProperties({
    entities: {},
    config,
    onFilterSelection: () => {}
  });

  this.render(hbs`
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

