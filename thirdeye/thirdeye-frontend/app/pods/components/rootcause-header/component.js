import Component from '@ember/component';

export default Component.extend({
  sessionId: null, // 0

  sessionName: null, // ""

  sessionText: null, // ""

  sessionModified: null, // true

  onSave: null, // func(sessionName, sessionText)

  onCopy: null, // func(sessionName, sessionText)

  actions: {
    onSave() {
      const { onSave } = this.getProperties('onSave');
      onSave();
    },

    onCopy() {
      const { onCopy } = this.getProperties('onCopy');
      onCopy();
    },

    onChange() {
      const { sessionName, sessionText, onChange } =
        this.getProperties('sessionName', 'sessionText', 'onChange');
      onChange(sessionName, sessionText);
    }
  }
});
