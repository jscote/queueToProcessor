/**
 * Created by jean-sebastiencote on 1/11/15.
 */
(function (_, Processor, q) {

    'use strict';

    function executeProcessor(processorName, msg, deferred, msgType) {
        Processor.getProcessor(processorName).then(function (processor) {
            processor.execute(msg).then(function (result) {
                deferred.resolve(result);
            }).fail(function (error) {
                deferred.reject(error);
            });
        }).fail(function (error) {
            deferred.reject(error + ' :: msgType/processor = ' + msgType);
        });
    }

    module.exports = function (queueConfig) {

        if (_.isUndefined(queueConfig) ||
            _.isNull(queueConfig) ||
            _.isUndefined(queueConfig.types) ||
            _.isNull(queueConfig.types)) return;

        for (var i = 0; i < queueConfig.types.length; i++) {
            var currentType = queueConfig.types[i];
            if (!_.isUndefined(currentType.mapToProcessor)) {
                currentType.listener = (function (type) {
                    var msgType = type;
                    return function (msg) {
                        var deferred = q.defer();

                        var processorName = msgType;

                        if(_.isFunction(currentType.mapToProcessor)) {
                            q.fcall(currentType.mapToProcessor, {messageType: msgType, message: msg}.then(function(name) {
                                executeProcessor(name, msg, deferred, msgType);
                            }));
                        } else {
                            executeProcessor(processorName, msg, deferred, msgType);
                        }
                        return deferred.promise;
                    }
                }(currentType.type));
            }
        }
    }

})(require('lodash'), require('jsai-jobprocessor').Processor, require('q'));