import $ from 'jquery';
import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';

module('Integration | Component | entity-filter', function(hooks) {
  setupRenderingTest(hooks);

  test('Filter blocks rendering properly', async function(assert) {
    this.setProperties({
      resetFilters: null,
      filterBlocksGlobal: [
        {
          name: 'primary',
          type: 'link',
          preventCollapse: true,
          totals: [5, 10, 15],
          selected: ['All alerts'],
          filterKeys: ['Alerts I subscribe to', 'Alerts I own', 'All alerts']
        }
      ],
      filterBlocksLocal: [
        {
          name: 'status',
          title: 'Status',
          type: 'checkbox',
          selected: ['Active', 'Inactive'],
          filterKeys: ['Active', 'Inactive']
        },
        {
          name: 'application',
          title: 'Applications',
          type: 'select',
          matchWidth: true,
          filterKeys: ['app1', 'app2', 'app3']
        },
        {
          name: 'subscription',
          title: 'Subscription Groups',
          type: 'select',
          filterKeys: ['group1', 'group2', 'group3']
        }
      ],
      userDidSelectFilter: () => {}
    });

    await render(hbs`
      {{entity-filter
        title="Quick Filters"
        maxStrLen=25
        resetFilters=resetFilters
        filterBlocks=filterBlocksGlobal
        onSelectFilter=(action userDidSelectFilter)
      }}
    `);

    const $filterBlock = this.$('.entity-filter');
    const $blockTitle = this.$('.entity-filter__head').get(0);
    const $filterList = this.$('.entity-filter__group-list--link li');

    assert.ok(
      $filterBlock.length,
      'Global filter block renders'
    );

    assert.ok(
      $blockTitle.innerText.includes('Quick Filters'),
      'Block title renders ok'
    );

    $filterList.each((index, item) => {
      let itemName = this.filterBlocksGlobal[0].filterKeys[index];
      let itemTotal = this.filterBlocksGlobal[0].totals[index];
      assert.equal(
        item.innerText.trim(),
        `${itemName} (${itemTotal})`,
        `correctly displays filter item "${itemName}" and its count`
      );
    });

    await render(hbs`
      {{entity-filter
        title="Filters"
        maxStrLen=25
        selectDisabled=isSelectDisabled
        resetFilters=resetFilters
        filterBlocks=filterBlocksLocal
        onSelectFilter=(action userDidSelectFilter)
      }}
    `);

    const $activityFilters = this.$('.entity-filter__group-list--checkbox .entity-filter__group-filter');
    const $selectFilters = this.$('section.entity-filter__group--select');
    const statusFilterBlock = this.filterBlocksLocal.find(filter => filter.name === 'status');
    const selectFilterBlocks = this.filterBlocksLocal.filter(filter => filter.type === 'select');

    assert.ok(
      $activityFilters.length === statusFilterBlock.filterKeys.length,
      'Status filter block renders'
    );

    $activityFilters.each((index, el) => {
      assert.ok(
        $(el).find('input').get(0).checked === true,
        `Status filter activity checkbox ${index} checked`
      );
      assert.ok(
        $(el).find('label').get(0).innerText.includes(statusFilterBlock.filterKeys[index]),
        `Status filter activity labels render correctly for ${statusFilterBlock.filterKeys[index]}`
      );
    });

    assert.ok(
      $selectFilters.length === selectFilterBlocks.length,
      'Select input sections rendered'
    );

    $selectFilters.each((index, section) => {
      assert.ok(
        $(section).find('.entity-filter__group-title').get(0).innerText.includes(selectFilterBlocks[index].title),
        `Select filter title ${selectFilterBlocks[index].title} displays correctly.`
      );
      assert.ok(
        $(section).find('.ember-power-select-multiple-trigger').length,
        `Select input appears for ${selectFilterBlocks[index].name}`
      );
    });
  });
});
