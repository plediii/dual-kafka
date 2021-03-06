"use strict";

var _ = require('lodash');

module.exports = function (hooks) {

    var hook = function (hookName, args, _this) {
        if (hooks.hasOwnProperty(hookName)) {
            return hooks[hookName].apply(_this, args);
        }
    };

    var Client = function (connectionString, clientId, zkOptions) {
        this.connectionString = connectionString;
        this.clientId = clientId;
        this.zkOptions = zkOptions;
        hook('clientConstructor', arguments, this);
    };

    var HighLevelConsumer = function (client, payloads, options) {
        this.client = client;
        this.payloads = payloads;
        this.options = options;
        this.callbacks = { 'message': [] };
        hook('highLevelConsumerConstructor', arguments, this);
    };

    _.extend(HighLevelConsumer.prototype, {
        on: function (name, onMessage) {
            this.callbacks[name].push(onMessage);
        }
        , _recv: function (message) {
            var _this = this;
            process.nextTick(function () {
                _.each(_this.callbacks['message'], function (send) {
                    send(message);
                });
            });
        }
    });

    var HighLevelProducer = function (client, options) {
        this.client = client;
        this.options = options;
        hook('highLevelProducerConstructor', arguments, this);
    };

    _.extend(HighLevelProducer.prototype, {
        send: function (payloads, cb) {
            hook('highLevelProducerSend', [payloads], this);
            var result = hook('highLevelProducerReply', payloads);
            if (result) {
                return cb.apply(null, result);
            } else {
                return cb();
            }
        }
    });

    return {
        Client: Client
        , HighLevelConsumer: HighLevelConsumer
        , HighLevelProducer: HighLevelProducer
    };
};
