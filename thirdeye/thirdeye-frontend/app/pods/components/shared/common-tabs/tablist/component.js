/**
 * @description Tablist Component
 * @summary Displays a tablist body. This is meant for usage with `Common-tabs` component.
 * @module components/shared/common-tabs/tablist
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
 * @exports tablist
 */
import Component from '@ember/component';

export default Component.extend({
  tagName: 'ul',
  classNames: ['common-tabs__header']
});
