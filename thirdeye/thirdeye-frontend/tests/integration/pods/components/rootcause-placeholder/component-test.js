import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('rootcause-placeholder', 'Integration | Component | rootcause placeholder', {
  integration: true
});

test('it renders', function(assert) {
  const testMessage = 'Hey There!';
  this.set('message', testMessage);

  this.render(hbs`
    {{rootcause-placeholder 
      message=message}}
  `);
  const message = this.$().find('p').text();

  assert.equal(
    message,
    testMessage,
    'Message should be displayed correctly.'
  );
});
