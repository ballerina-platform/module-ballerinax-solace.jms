import ballerinax/solace.jms as jms;

jms:Service invalidOnMessageRest = @jms:ServiceConfig {queueName: "q"} service object {
    remote function onMessage(jms:Message message, string... extras) returns error? {}
};

jms:Service invalidOnErrorRest = @jms:ServiceConfig {queueName: "q"} service object {
    remote function onMessage(jms:Message message) returns error? {}
    remote function onError(jms:Error err, string... extras) returns error? {}
};
