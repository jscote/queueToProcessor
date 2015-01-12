/**
 * Created by jean-sebastiencote on 11/26/14.
 */

(function (util) {

    'use strict';

    var MessageBase = function MessageBase(options) {
        options = options || { };
        Object.defineProperty(this, "data", { writable: true, value: options.data || {} });
        Object.defineProperty(this, "correlationId", {writable: true, value: null});
        Object.defineProperty(this, "identity", {writable: true, value: null});
    };

    MessageBase.prototype.generateUUID = function () {
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
            var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
            return v.toString(16);
        });
    };

    MessageBase.prototype.setCorrelationId = function(){
        this.correlationId = this.generateUUID();
    };

    MessageBase.prototype.toJSON = function() {
        return {correlationId: this.correlationId, data: this.data, identity: this.identity};
    };

    MessageBase.prototype.fromJSON = function(obj){
        this.correlationId = obj.correlationId || this.correlationId;
        this.data = obj.data || this.data;
        this.identity = obj.identity ||this.identity;
    };

    var ServiceMessage = function ServiceMessage(options) {
        MessageBase.call(this, options);
    };

    util.inherits(ServiceMessage, MessageBase);

    var ServiceResponse = function ServiceResponse(options) {
        MessageBase.call(this, options);
        Object.defineProperty(this, "isSuccess", { writable: true, value: true });
        Object.defineProperty(this, "errors", { writable: true, value: [] });
    };

    util.inherits(ServiceResponse, MessageBase);

    ServiceResponse.prototype.toJSON = function() {
        return {correlationId: this.correlationId, data: this.data, identity: this.identity, isSuccess: this.isSuccess, errors: this.errors};
    };

    ServiceResponse.prototype.fromJSON = function(obj){
        this.correlationId = obj.correlationId || this.correlationId;
        this.data = obj.data || this.data;
        this.identity = obj.identity ||this.identity;
        this.isSuccess = obj.isSuccess;
        this.errors = obj.errors || this.errors;
    };



    module.exports.ServiceMessage = ServiceMessage;
    module.exports.ServiceResponse = ServiceResponse;

})(require('util'));