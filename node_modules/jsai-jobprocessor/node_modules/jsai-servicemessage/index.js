/**
 * Created by jean-sebastiencote on 11/26/14.
 */

(function (util, EventEmitter, _) {

    'use strict';

    var internalConfig;

    var MessageBase = function MessageBase(options) {
        EventEmitter.call(this);
        options = options || {};
        Object.defineProperty(this, "data", {writable: true, value: options.data || {}});
        Object.defineProperty(this, "correlationId", {writable: true, value: null});
        Object.defineProperty(this, "identity", {writable: true, value: null});

        this.listenToEvents();
    };

    MessageBase.config = function (config) {
        internalConfig = config;
    };

    util.inherits(MessageBase, EventEmitter);

    MessageBase.prototype.listenToEvents = function () {
        if(!_.isUndefined(internalConfig)) {
            if (!_.isUndefined(internalConfig.messageCreatedHandler) &&
                _.isFunction(internalConfig.messageCreatedHandler)) {
                this.on('message.created', function (args) {
                    process.nextTick(function () {
                        internalConfig.messageCreatedHandler(args);
                    });
                });
            }

            if (!_.isUndefined(internalConfig.messageUpdatedHandler) &&
                _.isFunction(internalConfig.messageUpdatedHandler)) {
                this.on('message.updated', function (args) {
                    process.nextTick(function () {
                        internalConfig.messageUpdatedHandler(args);
                    });
                });
            }
        }

    };

    MessageBase.prototype.update = function() {
        this.emit('message.updated', this);
    };

    MessageBase.prototype.generateUUID = function () {
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
            var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
            return v.toString(16);
        });
    };

    MessageBase.prototype.setCorrelationId = function () {
        this.correlationId = this.generateUUID();
    };

    MessageBase.prototype.toJSON = function () {
        return {correlationId: this.correlationId, data: this.data, identity: this.identity};
    };

    MessageBase.prototype.fromJSON = function (obj) {
        this.correlationId = obj.correlationId || this.correlationId;
        this.data = obj.data || this.data;
        this.identity = obj.identity || this.identity;
    };

    var ServiceMessage = function ServiceMessage(options) {
        MessageBase.call(this, options);
        this.emit('message.created', this);
    };

    util.inherits(ServiceMessage, MessageBase);

    var ServiceResponse = function ServiceResponse(options) {
        MessageBase.call(this, options);
        Object.defineProperty(this, "isSuccess", {writable: true, value: true});
        Object.defineProperty(this, "errors", {writable: true, value: []});
        Object.defineProperty(this, "warnings", {writable: true, value: []});
        Object.defineProperty(this, "hasErrors", {
            get: function () {
                return this.errors.length > 0;
            }
        });
        Object.defineProperty(this, "hasWarnings", {
            get: function () {
                return this.warnings.length > 0;
            }
        });

        this.emit('message.created', this);

    };

    util.inherits(ServiceResponse, MessageBase);

    ServiceResponse.prototype.toJSON = function () {
        return {
            correlationId: this.correlationId,
            data: this.data,
            identity: this.identity,
            isSuccess: this.isSuccess,
            errors: this.errors
        };
    };

    ServiceResponse.prototype.fromJSON = function (obj) {
        this.correlationId = obj.correlationId || this.correlationId;
        this.data = obj.data || this.data;
        this.identity = obj.identity || this.identity;
        this.isSuccess = obj.isSuccess;
        this.errors = obj.errors || this.errors;
    };

    ServiceResponse.prototype.addError = function (error) {
        this.errors.push(error);
        this.isSuccess = false;
    };

    ServiceResponse.prototype.addWarning = function (warning) {
        this.warnings.push(warning);
    };


    module.exports.ServiceMessage = ServiceMessage;
    module.exports.ServiceResponse = ServiceResponse;
    module.exports.configure = MessageBase.config;

})(require('util'), require('events').EventEmitter, require('lodash'));