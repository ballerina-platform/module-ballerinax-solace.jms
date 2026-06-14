// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// End-to-end example for the SMF-native API (`ballerinax/solace.smf`).
//
// It exercises the capabilities the JMS surface cannot expose:
//   1. A persistent publisher with a broker publish receipt (guaranteed delivery).
//   2. A persistent receiver that provisions its own queue (CREATE_ON_START), binds a topic
//      subscription, performs structured data binding, and settles messages explicitly.
//   3. Negative settlement: a poison message is REJECTED so it is not redelivered.
//   4. Native request-reply between a Requester and a Replier.
//
// Run a Solace PubSub+ broker first, e.g.:
//   docker run -d -p 55554:55555 -p 8080:8080 --shm-size=1g \
//     -e username_admin_globalaccesslevel=admin -e username_admin_password=admin \
//     solace/solace-pubsub-standard:10.25.24

import ballerina/io;
import ballerinax/solace.smf;

configurable string brokerUrl = "smf://localhost:55554";
configurable string username = "admin";
configurable string password = "admin";

const string ORDER_TOPIC = "orders/new";
const string ORDER_QUEUE = "example-orders-queue";
const string PRICING_TOPIC = "pricing/quote";

type Order record {|
    string id;
    string item;
    int quantity;
|};

type OrderMessage record {|
    *smf:Message;
    Order payload;
|};

public function main() returns error? {
    io:println("=== Solace SMF order-processing example ===\n");

    check runPersistentPipeline();
    check runNegativeSettlement();
    check runRequestReply();

    io:println("\n=== Example completed successfully ===");
}

// 1 + 2: guaranteed publish, self-provisioned queue, data binding, explicit ack.
function runPersistentPipeline() returns error? {
    io:println("[1] Persistent publish + receive with data binding and ack");

    // The receiver provisions the queue and binds the order topic to it on start-up.
    smf:PersistentReceiver receiver = check new (brokerUrl,
        auth = {username, password},
        queueName = ORDER_QUEUE,
        topicSubscriptions = [ORDER_TOPIC],
        missingResourcesStrategy = smf:CREATE_ON_START
    );

    smf:PersistentPublisher publisher = check new (brokerUrl, auth = {username, password});

    Order[] orders = [
        {id: "ORD-1", item: "keyboard", quantity: 2},
        {id: "ORD-2", item: "monitor", quantity: 1}
    ];
    foreach Order 'order in orders {
        // publish() blocks until the broker acknowledges the message (publish receipt)
        check publisher->publish('order, ORDER_TOPIC);
        io:println(string `    published ${'order.id} (receipt confirmed by broker)`);
    }
    check publisher->close();

    foreach int _ in 0 ..< orders.length() {
        OrderMessage? message = check receiver->receive(10);
        if message is () {
            return error("expected an order message but none arrived");
        }
        Order received = message.payload;
        io:println(string `    received ${received.id}: ${received.quantity} x ${received.item}` +
            string ` (redelivered=${message.redelivered ?: false})`);
        check receiver->ack(message);
    }
    check receiver->close();
    io:println("    ✓ all orders received and acknowledged\n");
}

// 3: REJECTED negative settlement removes a poison message without redelivery.
function runNegativeSettlement() returns error? {
    io:println("[2] Negative settlement (REJECTED) on a poison message");

    smf:PersistentReceiver receiver = check new (brokerUrl,
        auth = {username, password},
        queueName = ORDER_QUEUE,
        topicSubscriptions = [ORDER_TOPIC],
        missingResourcesStrategy = smf:CREATE_ON_START,
        negativeSettlementEnabled = true
    );

    smf:PersistentPublisher publisher = check new (brokerUrl, auth = {username, password});
    check publisher->publish({id: "ORD-BAD", item: "unknown", quantity: -1}, ORDER_TOPIC);
    check publisher->close();

    OrderMessage? message = check receiver->receive(10);
    if message is () {
        check receiver->close();
        return error("expected the poison message but none arrived");
    }
    io:println(string `    received poison order ${message.payload.id}, rejecting it`);
    check receiver->rejected(message);

    // A rejected message must not be redelivered.
    smf:Message? next = check receiver->receive(3);
    check receiver->close();
    if next is () {
        io:println("    ✓ rejected message was not redelivered\n");
    } else {
        return error("rejected message was unexpectedly redelivered");
    }
}

// 4: native request-reply.
function runRequestReply() returns error? {
    io:println("[3] Native request-reply (pricing service)");

    // Create the replier first: its init establishes the topic subscription before returning, so
    // the request below cannot race ahead of the subscription and be lost.
    smf:Replier replier = check new (brokerUrl,
        auth = {username, password},
        topicSubscription = PRICING_TOPIC
    );
    future<error?> pricingService = start servePricingRequest(replier);

    smf:Requester requester = check new (brokerUrl, auth = {username, password});
    // The request leg of request-reply uses direct (at-most-once) messaging, so retry if a request
    // is dropped before the replier's flow is fully ready.
    smf:Message? quote = ();
    foreach int _ in 0 ..< 5 {
        smf:Message|smf:Error attempt = requester->request("keyboard", PRICING_TOPIC, replyTimeout = 5);
        if attempt is smf:Message {
            quote = attempt;
            break;
        }
    }
    check requester->close();
    if quote is () {
        return error("no pricing reply received after retries");
    }

    io:println(string `    quote received: ${quote.payload.toString()}`);
    lock {
        pricingDone = true;
    }
    check wait pricingService;
    io:println("    ✓ request-reply round-trip completed\n");
}

isolated boolean pricingDone = false;

// Answers pricing requests in a loop until the requester has its quote, so a retried request
// (the request leg is at-most-once) is still served.
function servePricingRequest(smf:Replier replier) returns error? {
    while true {
        boolean done;
        lock {
            done = pricingDone;
        }
        if done {
            break;
        }
        smf:Message? request = check replier->receive(2);
        if request is () {
            continue;
        }
        string item = request.payload.toString();
        check replier->reply(request, string `price of ${item} = $49.99`);
    }
    check replier->close();
}
