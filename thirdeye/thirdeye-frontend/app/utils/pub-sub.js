/** A very lightweight pub-sub implementation to communicate between ember-model-table
 *  and the hosting component
 *
 *  Usage:
 *  import pubSub from 'app/utils/pub-sub';
 *
 *  For publishing
 *  pubSub.publish('testEvent', data);
 *
 *  For subscribing
 *  const subscription = pubSub.subscribe('testEvent', (data) => {
 *  });
 *
 *  For unsubscribing
 *  subscription.unSubscribe();
 */
class PubSub {
  constructor() {
    this.subscribers = {};
  }

  /**
   * Publish the event to the Pubsub which will trigger all subscribers listening on it.
   *
   * @public
   *
   * @param {String} eventName
   *   The name of the event
   * @param {Any} data
   *   The data to pass in to the listeners of the event
   */
  publish(eventName, data) {
    if (!Array.isArray(this.subscribers[eventName])) {
      return;
    }

    this.subscribers[eventName].forEach((callback) => {
      callback(data);
    });
  }

  /**
   * Subscribe for an event to listen to.
   *
   * @public
   *
   * @param {String} eventName
   *   The name of the event
   * @param {Function} callback
   *   The callback function to be invoked when a publisher publishes the event that the subscriber is
   *   listening for
   *
   * @returns {Object}
   *   The object holds reference to the unSubscribe function
   */
  subscribe(eventName, callback) {
    if (!Array.isArray(this.subscribers[eventName])) {
      this.subscribers[eventName] = [];
    }

    this.subscribers[eventName].push(callback);
    const index = this.subscribers[eventName].length - 1;

    return {
      unSubscribe: () => {
        this.subscribers[eventName].splice(index, 1);
      }
    };
  }
}

export default new PubSub();
