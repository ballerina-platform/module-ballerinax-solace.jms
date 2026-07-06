import ballerina/log;
import ballerinax/solace.jms as jms;

configurable string brokerUrl = "smf://localhost:55554";
configurable string messageVpn = "default";
configurable string username = "admin";
configurable string password = "admin";

const string ORDERS_QUEUE = "orders";

type Order record {|
    string orderId;
    string item;
    int quantity;
|};

public function main() returns error? {
    jms:MessageProducer producer = check new (brokerUrl, {
        messageVpn,
        auth: {username, password},
        enableDynamicDurables: true,
        directTransport: false,
        destination: {queueName: ORDERS_QUEUE}
    });

    Order[] orders = [
        {orderId: "ORD-1001", item: "Wireless Mouse", quantity: 2},
        {orderId: "ORD-1002", item: "Mechanical Keyboard", quantity: 1},
        {orderId: "ORD-1003", item: "USB-C Hub", quantity: 3}
    ];

    foreach Order 'order in orders {
        check producer->send({payload: 'order});
        log:printInfo("Order placed", orderId = 'order.orderId, item = 'order.item);
    }

    check producer->close();
}
