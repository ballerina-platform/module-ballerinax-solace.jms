import ballerinax/solace.jms as jms;

function acceptService(jms:Service solaceService) {
}

function passService() {
    acceptService(service object {
        remote function onMessage(jms:Message message) returns error? {}
    });
}
