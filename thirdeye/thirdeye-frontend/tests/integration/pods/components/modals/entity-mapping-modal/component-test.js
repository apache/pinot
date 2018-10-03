import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import { clickTrigger, selectChoose, findContains } from 'ember-power-select/test-support/helpers';
import hbs from 'htmlbars-inline-precompile';
import $ from 'jquery';


module('Integration | Component | modals/entity mapping modal', function(hooks) {
  setupRenderingTest(hooks);

  const submitFunction = () => {};
  const metric = {
    urn: 'thirdeye:metric:1234567',
    label: "test_metric",
    type: "metric"
  };

  test('it renders', async function(assert) {
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

    const $modalBody = $('.te-modal__body');
    assert.equal($('.te-modal__header').text().trim(), `Configure Filters for analyzing test_metric`);
    assert.ok($modalBody.find('.te-modal__table').length, 'component should have a table');

    clickTrigger('#select-mapping-type');
    assert.ok($('.ember-power-select-dropdown').length, 'Dropdown is rendered');
  });

  test('simple mapping works', async function (assert) {
    const okSelector = '#simple-entity-selection';
    const notOkSelector = '#advanced-entity-selection';
    this.setProperties({
      showEntityMappingModal: true,
      onSubmit: submitFunction,
      metric
    });

    await render(hbs`
      {{modals/entity-mapping-modal
        showEntityMappingModal=showEntityMappingModal
        metric=metric
        selectedMappingType="lixtag"
        onSubmit=onSubmit
      }}
    `);

    assert.ok(
      $(okSelector).length,
      `${okSelector} should render properly when selecting`);
    assert.notOk(
      $(notOkSelector).length,
      `${notOkSelector} should not render when selecting`);
  });

  test('advanced mapping works', async function (assert) {
    const okSelector = '#advanced-entity-selection';
    const notOkSelector = '#simple-entity-selection';
    this.setProperties({
      showEntityMappingModal: true,
      onSubmit: submitFunction,
      metric
    });

    await render(hbs`
      {{modals/entity-mapping-modal
        showEntityMappingModal=showEntityMappingModal
        metric=metric
        selectedMappingType="service"
        onSubmit=onSubmit
      }}
    `);

    assert.ok(
      $(okSelector).length,
      `${okSelector} should render properly when selecting`);
    assert.notOk(
      $(notOkSelector).length,
      `${notOkSelector} should not render when selecting`);
  });
});
