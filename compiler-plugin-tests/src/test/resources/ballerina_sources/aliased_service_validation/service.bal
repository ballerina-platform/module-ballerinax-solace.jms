import ballerinax/solace.jms as jms;

type AliasedService jms:Service;

AliasedService missingObjectServiceConfig = service object {
    remote function onMessage(jms:Message message) returns error? {}
};

type AliasedListener jms:Listener;

listener AliasedListener aliasedListener = check new ("tcp://localhost:55554");

service on aliasedListener {
    remote function onMessage(jms:Message message) returns error? {}
}
