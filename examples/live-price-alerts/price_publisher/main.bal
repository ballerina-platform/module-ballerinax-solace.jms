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

public function main() returns error? {
    // A producer is bound to a single destination for its lifetime, so each topic
    // gets its own short-lived producer here.
    check publish("stocks/nasdaq/aapl", {symbol: "AAPL", price: 227.50d, changePercent: 2.1});
    check publish("stocks/nasdaq/googl", {symbol: "GOOGL", price: 168.90d, changePercent: 6.8});
    check publish("stocks/nyse/ibm", {symbol: "IBM", price: 231.40d, changePercent: 9.4});
}

function publish(string topic, PriceUpdate update) returns error? {
    jms:MessageProducer producer = check new (brokerUrl, {
        messageVpn,
        auth: {username, password},
        // Solace direct messaging does not support message selectors, and the
        // subscriber uses one, so this producer must use guaranteed delivery too.
        directTransport: false,
        destination: {topicName: topic}
    });

    check producer->send({
        payload: update,
        properties: {"changePercent": update.changePercent}
    });
    check producer->close();

    log:printInfo("Price update published", topic = topic, symbol = update.symbol, changePercent = update.changePercent);
}
