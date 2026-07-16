import ballerinax/solace.jms as jms;

jms:Service missingAnnotation = service object {
    remote function onMessage(jms:Message message) returns error? {}
};
