import { module, test, skip } from 'qunit';
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
      },
      onCancel: () => {},
      onSave: () => {}
    });

    await render(hbs`
      {{#te-modal
        isMicroModal=true
        cancelButtonText="Cancel"
        submitButtonText="Save"
        submitAction=(action "onSave")
        cancelAction=(action "onCancel")
        isShowingModal=true
        headerText=modalTitle
      }}
        {{rootcause-dimensions-settings
          dimensionOptions=dimensionOptions
          customTableSettings=customTableSettings
        }}
      {{/te-modal}}
    `);

    assert.ok($(genEl.MICRO_MODAL).get(0).length, 'Modal renders properly');

    assert.equal(
      $(enEl.MODAL_TITLE).get(0).innerText,
      modalTitle,
      'Modal title renders correctly'
    );

    // TODO: more assertions to come...
  });
});
