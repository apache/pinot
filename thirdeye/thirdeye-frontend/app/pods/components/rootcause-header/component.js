import Component from '@ember/component';
import { get, getProperties, computed } from '@ember/object';
import { dateFormatFull } from 'thirdeye-frontend/utils/rca-utils';
import moment from 'moment';

export default Component.extend({
  classNames: ['rootcause-header'],

  /**
   * Rootcause session identifier
   * @type {Int}
   */
  sessionId: null,

  /**
   * Rootcause session title
   * @type {string}
   */
  sessionName: null,

  /**
   * Rootcause session comments
   * @Type {string}
   */
  sessionText: null,

  /**
   * Rootcause session owner
   * @Type {string}
   */
  sessionOwner: null,

  /**
   * Rootcause session dirty flag
   * @type {boolean}
   */
  sessionModified: null,

  /**
   * Rootcause session last updated timestamp
   * @type {Int}
   */
  sessionUpdatedTime: null,

  /**
   * Rootcause session last updated author
   * @type {string}
   */
  sessionUpdatedBy: null,

  /**
   * "Save" button callback
   * @type {function}
   */
  onSave: null, // func()

  /**
   * "Copy" button callback
   * @type {function}
   */
  onCopy: null, // func()

  /**
   * Text edit callback
   * @type {function}
   */
  onChange: null, // func(sessionName, sessionText)

  /**
   * Toggle for the comment textarea
   * @type {boolean}
   */
  isCommentEditMode: computed.bool('sessionText'),

  /**
   * Toggle for editing the session title
   * @type {boolean}
   */
  isTitleEditMode: false,

  /**
   * whether to enable saving
   * @type {boolean}
   */
  canSave: false,

  /**
   * whether to enable copying
   * @type {boolean}
   */
  canCopy: false,

  /**
   * Formatted session last updated time
   * @type {string}
   */
  sessionUpdatedTimeFormatted: computed('sessionUpdatedTime', function () {
    return moment(get(this, 'sessionUpdatedTime')).format(dateFormatFull);
  }),

  actions: {
    /**
     * "Save" button action
     */
    onSave() {
      const { onSave } = getProperties(this, 'onSave');
      onSave();
    },

    /**
     * "Copy" button action (currently disabled)
     */
    onCopy() {
      const { onCopy } = getProperties(this, 'onCopy');
      onCopy();
    },

    /**
     * Edit action on changing session title or comments
     */
    onChange() {
      const {
        sessionName,
        sessionText,
        onChange
      } = getProperties(this, 'sessionName', 'sessionText', 'onChange');

      onChange(sessionName, sessionText);
    }
  }
});
