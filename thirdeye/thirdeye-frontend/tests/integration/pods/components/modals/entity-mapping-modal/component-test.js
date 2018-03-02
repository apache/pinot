import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import $ from 'jquery';

moduleForComponent('modals/entity-mapping-modal', 'Integration | Component | modals/entity mapping modal', {
  integration: true
});

test('it renders', function(assert) {
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

  this.render(hbs`
    {{modals/entity-mapping-modal
      showEntityMappingModal=showEntityMappingModal
      metric=metric
      onSubmit=onSubmit
    }}
  `);

  assert.equal($('.te-modal__header').text().trim(), `Configure Filters for analyzing ${metric.label}`);
  assert.ok($('.te-modal__body').find('.te-modal__table').length, 'component should have a table');
});
