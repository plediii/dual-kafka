/*jslint node: true */
"use strict";

var Promise = require('bluebird');
var _ = require('lodash');
var assert = require('assert');
var dualproto = require('dual-protocol');

var kafkaNode = require('./mock-kafka');

describe('dual-kafka', function () {

    var d;
    var kafkaHooks;
    beforeEach(function () {
        kafkaHooks = {};
        d = dualproto
            .use(function (Domain, libs) {
                libs['kafka-node'] = kafkaNode(kafkaHooks)
            })
            .use(require('../index'))();
    });

    describe('kafkaClient', function () {
        it('should be available on domain', function () {
            assert(_.isFunction(d.kafkaClient));
        });

        it('should call the kafka-node constructor', function (done) {
            kafkaHooks.clientConstructor = function (connectionString, clientId, zkOptions) {
                assert.equal(connectionString, 'zookeeper:2190');
                assert.equal(clientId, 'test-client');
                assert.equal(zkOptions.back, 'indeed');
                done();
            };
            d.kafkaClient({
                connectionString: 'zookeeper:2190'
                , clientId: 'test-client'
                , zkOptions: {
                    back: 'indeed'
                }
            });
        });
    });

    describe('client', function () {
        var kafkaClient;
        beforeEach(function () {
            kafkaClient = d.kafkaClient();
        });

        it('should be available on the client object', function () {
            assert(_.isFunction(kafkaClient.consumer));
        });

        it('should construct a consumer from the client', function (done) {
            kafkaHooks.highLevelConsumerConstructor = function (client) {
                done();
            }
            kafkaClient.producer();
        });
    });

    describe('consumer', function () {
        var kafkaClient;
        beforeEach(function () {
            kafkaClient = d.kafkaClient();
        });
        
        describe('constructor', function () { 
            it('should be available on the client object', function () {
                assert(_.isFunction(kafkaClient.consumer));
            });

            it('should construct a consumer with provided topics', function (done) {
                kafkaHooks.highLevelConsumerConstructor = function (client, payloads) {
                    assert.equal(payloads[0].topic, 'target')
                    done();
                }
                kafkaClient.consumer([{ topic: 'target '}]);
            });

            it('should construct a consumer with provided options', function (done) {
                kafkaHooks.highLevelConsumerConstructor = function (client, payloads, options) {
                    assert.equal(options.right, 'on')
                    done();
                }
                kafkaClient.consumer([], { right: 'on' });
            });
        });

        describe('onMessage', function () {
            var mockConsumer;
            beforeEach(function () {
                kafkaHooks.highLevelConsumerConstructor = function () {
                    mockConsumer = this;
                };
                kafkaClient.consumer();
            });

            var mockSend = function () {
                mockConsumer._recv({
                    topic: 'promised',
                    value: '{"time":2440629141879,"value":96}',
                    offset: 197,
                    partition: 1,
                    key: new Buffer('key')
                });
            };

            it('should send messages to consumer routes', function (done) {
                d.mount(['kafka', 'consumer', '**'], function (body, ctxt) {
                    done();
                });
                mockSend();
            });

            it('should send messages to consumer routes with topic in options', function (done) {
                d.mount(['kafka', 'consumer', '**'], function (body, ctxt) {
                    assert.equal('promised', ctxt.options.kafka.topic);
                    done();
                });
                mockSend();
            });

            it('should send messages to consumer routes with offset in options', function (done) {
                d.mount(['kafka', 'consumer', '**'], function (body, ctxt) {
                    assert.strictEqual(197, ctxt.options.kafka.offset);
                    done();
                });
                mockSend();
            });

            it('should send messages to consumer routes with partition in options', function (done) {
                d.mount(['kafka', 'consumer', '**'], function (body, ctxt) {
                    assert.strictEqual(1, ctxt.options.kafka.partition);
                    done();
                });
                mockSend();
            });

            it('should send messages to consumer routes with message as body', function (done) {
                d.mount(['kafka', 'consumer', '**'], function (body, ctxt) {
                    assert.strictEqual(96, body.value);
                    done();
                });
                mockSend();
            });

            it('should send messages to specific topic subroute', function (done) {
                d.mount(['kafka', 'consumer', 'promised', '**'], function (body, ctxt) {
                    done();
                });
                mockSend();
            });

            it('should send messages to specific topic/partition subroute', function (done) {
                d.mount(['kafka', 'consumer', 'promised', '1'], function (body, ctxt) {
                    done();
                });
                mockSend();
            });
        });
    });

    describe('producer', function () {
        var kafkaClient;
        beforeEach(function () {
            kafkaClient = d.kafkaClient();
        });
        
        it('should be available on the client object', function () {
            assert(_.isFunction(kafkaClient.producer));
        });

        it('should construct a producer from the client', function (done) {
            kafkaHooks.highLevelProducerConstructor = function (client) {
                done();
            }
            kafkaClient.producer();
        });

        describe('send', function () {
            beforeEach(function () {
                kafkaClient.producer();
            });

            it('should be mounted at route producer', function () {
                assert(d.listeners(['kafka', 'producer', '*']).length > 0);
            });

            it('should initiate a producer send on a topic described by route', function (done) {
                kafkaHooks.highLevelProducerSend = function (payloads) {
                    assert.equal(payloads[0].topic, 'child');
                    done();
                };
                d.send(['kafka', 'producer', 'child'], [], { ancient: 'legend' });
            });

            it('should initiate a producer send on a with a stringified copy of the body', function (done) {
                kafkaHooks.highLevelProducerSend = function (payloads) {
                    var body = JSON.parse(payloads[0].messages[0]);
                    assert.equal(body.ancient, 'legend');
                    done();
                };
                d.send(['kafka', 'producer', 'child'], [], { ancient: 'legend' });
            });
        });
    });
});
