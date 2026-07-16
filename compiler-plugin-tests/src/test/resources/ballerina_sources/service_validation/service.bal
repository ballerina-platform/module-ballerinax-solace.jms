import ballerinax/solace.jms as jms;

jms:Service missingAnnotation = service object {
    remote function onMessage(jms:Message message) returns error? {}
};

jms:Service resourceMethod = @jms:ServiceConfig {queueName: "q"} service object {
    resource function get status() returns string => "ok";
    remote function onMessage(jms:Message message) returns error? {}
};

jms:Service missingOnMessage = @jms:ServiceConfig {queueName: "q"} service object {
};

jms:Service unsupportedMethod = @jms:ServiceConfig {queueName: "q"} service object {
    remote function onEvent(jms:Message message) returns error? {}
};

jms:Service invalidParameterCount = @jms:ServiceConfig {queueName: "q"} service object {
    remote function onMessage() returns error? {}
};

jms:Service invalidMessageType = @jms:ServiceConfig {queueName: "q"} service object {
    remote function onMessage(string payload) returns error? {}
};

jms:Service invalidCallerPosition = @jms:ServiceConfig {queueName: "q"} service object {
    remote function onMessage(jms:Message message, jms:Message second) returns error? {}
};

jms:Service invalidOnError = @jms:ServiceConfig {queueName: "q"} service object {
    remote function onMessage(jms:Message message) returns error? {}
    remote function onError(jms:Message err) returns error? {}
};

jms:Service invalidReturn = @jms:ServiceConfig {queueName: "q"} service object {
    remote function onMessage(jms:Message message) returns string => "invalid";
};

type StringMessage record {|
    *jms:Message;
    string payload;
|};

jms:Service validService = @jms:ServiceConfig {queueName: "q"} service object {
    remote function onMessage(StringMessage message, jms:Caller caller) returns jms:Error? {}
    remote function onError(jms:Error err) returns error? {}
};
