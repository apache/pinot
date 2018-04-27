/**
 * @description Common-tabs Component
 * @summary Renders tabs. Enable a user to switch between views within a given screen. Provides high-level organization of information into navigable categories.
 * @module components/shared/common-tabs
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
 * @exports common-tabs
 */
import Component from '@ember/component';

export default Component.extend({
  classNames: ['common-tabs']
});
