import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('te-navbar', 'Integration | Component | te navbar', {
  integration: true
});

const testItems = [
  {
    className: 'home',
    link: 'index',
    title: 'Home'
  },
  {
    className: 'linkedin',
    link: 'http://www.linkedin.com/',
    isCustomLink: true,
    title: 'LI Home'
  }
];

const navSelector = 'nav.te-nav';
const navListSelector = 'li.te-nav__item';
const globalNavClass = 'te-nav--global';
const subNavClass = 'te-nav--sub';
const logoSelector = '.te-nav__logo';
const homeLinkAnchorSelector = '.te-nav__item--home a';
const extLinkAnchorSelecotr = '.te-nav__item--linkedin a';

// Test for proper rendering of global navigation bar component
test('Nav bar renders new items of type = global', function(assert) {
  this.setProperties({ 'testItems': testItems });
  this.render(hbs`{{te-navbar navItems=testItems isGlobalNav=true navClass='te-nav--global'}}`);
  assert.equal(this.$(navListSelector).length, 2, 'Global nav renders ' + testItems.length + ' items');
  assert.equal(this.$(navSelector)[0].classList[1], globalNavClass, 'Global nav renders with correct class name');
  assert.equal(this.$(logoSelector).length, 1, 'Global nav renders logo');
  assert.equal(this.$(homeLinkAnchorSelector).text().trim(), 'Home', 'Nav renders link text');
  assert.equal(this.$(extLinkAnchorSelecotr)[0].href, testItems[1].link, 'Nav renders external link');
});

// Test for proper rendering of sub navigation bar component
test('Nav bar renders new items of type = subnav', function(assert) {
  this.setProperties({ 'testItems': testItems });
  this.render(hbs`{{te-navbar navItems=testItems navClass='te-nav--sub'}}`);
  assert.equal(this.$(navListSelector).length, 2, 'Sub nav renders ' + testItems.length + ' items');
  assert.equal(this.$(navSelector)[0].classList[1], subNavClass, 'Sub nav renders with correct class name');
  assert.equal(this.$(logoSelector).length, 0, 'Sub nav does not render logo');
});

