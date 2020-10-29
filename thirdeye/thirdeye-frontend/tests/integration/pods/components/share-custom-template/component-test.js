import $ from 'jquery';
import { module, test } from 'qunit';
import { run } from '@ember/runloop';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import wait from 'ember-test-helpers/wait';

module('Integration | Component | share custom template', function(hooks) {
  setupRenderingTest(hooks);

  test('Confirming that share-custom-template component renders and properties passed works properly', async function(assert) {

    // Prepare props for component
    this.setProperties({
      willDestroy() {},
      appName: 'MAXTIME',
      start: '1539626400000',
      end: '1539626400000',
      shareConfig: {
        groupName: 'someGroup',
        appName: 'someApp',
        chart: 'table',
        title: 'Title of metrics',
        entities: [
          [
            { type: 'label', value: 'Column name1', index: 0},
            { type: 'label', value: 'Column name2', index: 0},
            { type: 'label', value: 'Column name3', index: 0}
          ],
          [
            { type: 'label', value: 'Email', index: 1},
            { type: 'change', metrics: ['thirdeye:metric:11111'], offsets: ['current', 'wo1w'], summary: ['69.0M', '-2.03'], index: 1},
            { type: 'change', metrics: ['thirdeye:metric:22222'], offsets: ['current', 'wo1w'], summary: ['69.0M', '-2.03'], index: 1}
          ]
        ]
      }
    });

    // Rendering the component
    await render(hbs`
      {{share-custom-template
        appName=appNameDisplay
        start=start
        end=end
        config=shareConfig
      }}
    `);

    await wait();
    const $shareCustomeTemplate = this.$('.share-custom-template').get(0).classList[0];
    const $firstCustomTemplateHeader = this.$('.share-custom-template table th').get(0).innerText.trim();
    const $firstOffsetSummary = this.$('.share-custom-template table tr td p').get(1).innerText.trim();

    assert.equal(
      $shareCustomeTemplate,
      'share-custom-template',
      'The custom template table component rendered.');

    assert.equal(
      $firstCustomTemplateHeader,
      'Column name1',
      'The custom template table component first header shows correctly.');

    assert.equal(
      $firstOffsetSummary,
      '69.0M',
      'The custom template table component first offset summary value shows correctly.');
  });
});
