import Component from '@ember/component';

export default Component.extend({
  sessionId: null, // 0

  sessionName: null, // ""

  sessionText: null, // ""

  sessionModified: null, // true

  undoMessage: null, // ""

  onSave: null, // func(sessionName, sessionText)

  onCopy: null, // func(sessionName, sessionText)

  onUndo: null, // func()

  //
  // internal
  //
  name: null, // internal to avoid data-binding

  text: null, // internal to avoid data-binding

  didReceiveAttrs() {
    const { sessionName, sessionText } = this.getProperties('sessionName', 'sessionText');
    this.setProperties({ name: sessionName, text: sessionText });
  },

  actions: {
    onSave() {
      const { onSave } = this.getProperties('onSave');
      onSave();
    },

    onCopy() {
      const { onCopy } = this.getProperties('onCopy');
      onCopy();
    },

    onUndo() {
      const { onUndo } = this.getProperties('onUndo');
      onUndo();
    },

    onChange() {
      const { name, text, onChange } =
        this.getProperties('name', 'text', 'onChange');
      onChange(name, text);
    }
  }
});
