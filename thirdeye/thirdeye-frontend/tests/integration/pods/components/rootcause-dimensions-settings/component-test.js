import $ from 'jquery';
import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { generalConst as genEl } from 'thirdeye-frontend/tests/utils/constants';

module('Integration | Component | rootcause-dimensions-settings', function(hooks) {
  setupRenderingTest(hooks);

  const modalTitle = 'Dimension Analysis Settings';

  test('it renders', async function(assert) {
    this.setProperties({
      modalTitle,
      dimensionOptions: ['dimension1', 'dimension2', 'dimension3'],
      customTableSettings: {
        depth: '3',
        dimensions: [],
        summarySize: 20,
        oneSideError: 'false',
        excludedDimensions: []
      }
    });

    await render(hbs`
      {{rootcause-dimensions-settings
        dimensionOptions=dimensionOptions
        customTableSettings=customTableSettings
      }}`);

    const $selectInclude = this.$('#dimension-select-include').get(0);
    const $selectExclude = this.$('#dimension-select-exclude').get(0);
    const $settingsFields = this.$('#dimensions-settings').get(0);
    const $selectErrorLabels = this.$('#dimensions-error').get(0);
    const $selectErrorField = this.$('#dimension-select-error').find('.ember-power-select-selected-item').get(0);

    assert.ok(
      $selectInclude.innerText.trim().includes('Select a dimension to include')
    );

    assert.ok(
      $selectExclude.innerText.trim().includes('Select a dimension to exclude')
    );

    assert.ok(
      $settingsFields.innerText.includes('Number of top contributors')
    );

    assert.ok(
      $settingsFields.innerText.trim().includes('Levels of dimension (max 3)')
    );

    assert.ok(
      $settingsFields.innerText.trim().includes('Levels of dimension to slice by')
    );

    assert.ok(
      $selectErrorLabels.innerText.trim().includes('One side error')
    );

    assert.ok(
      $selectErrorLabels.innerText.trim().includes('One side error')
    );

    assert.equal(
      $selectErrorField.innerText.trim(),
      'false',
      'One-side-error default value is false'
    );

    // TODO: more assertions to come...
  });
});
