import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | rootcause placeholder', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    const testMessage = 'Hey There!';
    this.set('message', testMessage);

    await render(hbs`
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
});
