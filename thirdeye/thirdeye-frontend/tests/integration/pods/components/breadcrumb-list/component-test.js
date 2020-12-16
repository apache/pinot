import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('breadcrumb-list', 'Integration | Component | breadcrumb list', {
  integration: true
});

test('it renders', function (assert) {
  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });

  this.setProperties({
    items: [
      {
        title: 'breadcrumb 1',
        id: 1,
        isRoot: true
      },
      {
        title: 'breadcrumb 2',
        id: 2
      }
    ]
  });

  this.render(hbs`{{breadcrumb-list items=items}}`);

  const displayedString = this.$().text();

  assert.ok(displayedString.includes(this.items[0].title), 'breadcrumb 1 is displayed');
  assert.ok(displayedString.includes(this.items[1].title), 'breadcrumb 2 is displayed');
  assert.ok(displayedString.includes('>'), 'separator is displayed');
});
