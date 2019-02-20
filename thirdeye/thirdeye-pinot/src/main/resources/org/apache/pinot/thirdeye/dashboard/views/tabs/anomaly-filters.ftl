{{#each anomaliesFilters}}
  <section class="filter-section" id="{{@key}}" data-section="{{@key}}">
    <h5 class="label-medium-semibold filter-title">
      <span class="filter-title__name">{{displayFilterName @key}}</span>
      <a class="filter-title__action {{#if this.expanded}}filter-title__action--expanded{{/if}}"></a>
    </h5>
    <ul class="filter-body__list {{#unless this.expanded}}filter-body__list--hidden{{/unless}}">

      {{#each this}}
        {{#if (isObject this)}}
         <section class="filter-section filter-section--no-padding" id="{{@key}}" data-section="{{@key}}">
          <h5 class="label-medium-semibold filter-title filter-title--small">
            <span class="filter-title__name">{{@key}}</span>
            <a class="filter-title__action {{#if this.expanded}}filter-title__action--expanded{{/if}}"></a>
          </h5>
            <ul class="filter-body__list {{#unless this.expanded}}filter-body__list--hidden{{/unless}}">
              {{#each this}}
                {{#if this.length}}
                   <li class="filter-item">
                    <input class="filter-item__checkbox" type="checkbox" id="{{@key}}" data-filter="{{@key}}" {{#if this.selected}} checked=true {{/if}}>
                    <label for="{{@key}}" class="filter-item__label" title="{{@key}}">{{@key}}</label>
                    <span class="filter-item__count">{{this.length}}</span>
                  </li>
                {{/if}}
              {{/each}}
            </ul>
          </section>
        {{else}}
          {{#if this.length}}
            <li class="filter-item">
              <input class="filter-item__checkbox" type="checkbox" id="{{@key}}" data-filter="{{@key}}" {{#if this.selected}} checked=true {{/if}}>
              <label for="{{@key}}" class="filter-item__label" title="{{@key}}">{{@key}}</label>
              <span class="filter-item__count">{{this.length}}</span>
            </li>
          {{/if}}
        {{/if}}
      {{/each}}
    </ul>
  </section>
{{/each}}
