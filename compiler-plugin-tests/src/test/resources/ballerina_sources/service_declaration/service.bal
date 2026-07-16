import ballerinax/solace.jms as jms;

listener jms:Listener solaceListener = check new ("tcp://localhost:55554");

service jms:Service on solaceListener {
    remote function onMessage(jms:Message message) returns error? {}
}

@jms:ServiceConfig {queueName: "orders"}
service jms:Service on solaceListener {
    remote function onMessage(jms:Message message) returns error? {}
}
