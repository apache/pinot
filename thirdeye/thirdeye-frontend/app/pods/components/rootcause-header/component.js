import Component from '@ember/component';
import { getProperties, computed } from '@ember/object';

export default Component.extend({
  classNames: ['rootcause-header'],
  sessionId: null, // 0

  sessionName: null, // ""

  sessionText: null, // ""

  sessionModified: null, // true

  onSave: null, // func(sessionName, sessionText)

  onCopy: null, // func(sessionName, sessionText)

  /**
   * Toggles the comment textarea
   */
  showComment: computed.bool('sessionText'),

  actions: {
    onSave() {
      const { onSave } = getProperties(this, 'onSave');
      onSave();
    },

    onCopy() {
      const { onCopy } = getProperties(this, 'onCopy');
      onCopy();
    },

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
