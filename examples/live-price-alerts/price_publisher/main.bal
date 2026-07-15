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
    // No fixed destination is configured here - the producer picks a destination per publish
    // call below, so one producer can fan out to every topic instead of one-per-topic.
    jms:MessageProducer producer = check new (brokerUrl, {
        messageVpn,
        auth: {username, password},
        // Solace direct messaging does not support message selectors, and the
        // subscriber uses one, so this producer must use guaranteed delivery too.
        directTransport: false
    });

    check publish(producer, "stocks/nasdaq/aapl", {symbol: "AAPL", price: 227.50d, changePercent: 2.1});
    check publish(producer, "stocks/nasdaq/googl", {symbol: "GOOGL", price: 168.90d, changePercent: 6.8});
    check publish(producer, "stocks/nyse/ibm", {symbol: "IBM", price: 231.40d, changePercent: 9.4});

    check producer->close();
}

function publish(jms:MessageProducer producer, string topic, PriceUpdate update) returns error? {
    check producer->send({
        payload: update,
        properties: {"changePercent": update.changePercent}
    }, {topicName: topic});

    log:printInfo("Price update published", topic = topic, symbol = update.symbol, changePercent = update.changePercent);
}
