import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import $ from 'jquery';

module('Integration | Component | modals/entity mapping modal', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders', async function(assert) {
    const submitFunction = () => {};
    const metric = {
      urn: 'thirdeye:metric:1234567',
      label: "test_metric",
      type: "metric"
    };
    this.setProperties({
      showEntityMappingModal: true,
      onSubmit: submitFunction,
      metric
    });

    await render(hbs`
      {{modals/entity-mapping-modal
        showEntityMappingModal=showEntityMappingModal
        metric=metric
        onSubmit=onSubmit
      }}
    `);

    assert.equal($('.te-modal__header').text().trim(), `Configure Filters for analyzing ${metric.label}`);
    assert.ok($('.te-modal__body').find('.te-modal__table').length, 'component should have a table');
  });
});
