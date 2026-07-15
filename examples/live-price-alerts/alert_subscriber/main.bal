import ballerina/log;
import ballerinax/solace.jms as jms;

configurable string brokerUrl = "smf://localhost:55554";
configurable string messageVpn = "default";
configurable string username = "admin";
configurable string password = "admin";

type PriceUpdate record {|
    string symbol;
    decimal price;
    float changePercent;
|};

listener jms:Listener alertListener = check new (brokerUrl, {
    messageVpn,
    auth: {username, password},
    // Solace direct messaging does not support message selectors, so this listener
    // (and the publisher's producer) must use guaranteed delivery instead.
    directTransport: false
});

// Subscribes to every symbol under the NASDAQ topic hierarchy, but only dispatches
// messages whose changePercent property indicates a significant move.
@jms:ServiceConfig {
    topicName: "stocks/nasdaq/*",
    messageSelector: "changePercent > 5.0"
}
service on alertListener {

    remote function onMessage(record {|*jms:Message; PriceUpdate payload;|} message) returns error? {
        PriceUpdate update = message.payload;
        log:printWarn("ALERT: significant price move", symbol = update.symbol, price = update.price,
                changePercent = update.changePercent);
    }
}
