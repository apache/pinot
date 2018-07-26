/**
 * @description Tab Component
 * @summary Displays a tablist tab body. This is meant for usage with `Common-tabs` component.
 * @module components/shared/common-tabs/tablist/tab
 * @example
 * @example
    {{#shared/common-tabs selection=activeTab activeTab=activeTab as |tabs|}}
      {{#tabs.tablist as |tablist|}}
        {{#tablist.tab name="metrics"}}Metrics{{/tablist.tab}}
        {{#tablist.tab name="dimensions"}}Dimensions{{/tablist.tab}}
      {{/tabs.tablist}}

      {{!-- metrics --}}
      {{#tabs.tabpanel name="metrics"}}
        {{!-- IMPORTANT metrics CODE --}}
      {{/tabs.tabpanel}}
      {{!-- dimensions --}}
      {{#tabs.tabpanel name="dimensions"}}
        {{!-- IMPORTANT dimensions CODE --}}
      {{/tabs.tabpanel}}
    {{/shared/common-tabs}}
 *
 * @exports tab
 */
import Component from '@ember/component';

export default Component.extend({
  tagName: 'li',
  classNames: ['common-tabs__subnav']
});
