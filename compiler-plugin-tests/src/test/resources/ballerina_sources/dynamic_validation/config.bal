import ballerinax/solace.jms as jms;

type QueueConfiguration record {|
    string queueName?;
    string durability;
|};

QueueConfiguration foreignQueue = {durability: "DURABLE"};

function dynamicQueue(jms:Durability durability, string queueName) returns jms:QueueConfiguration {
    return {durability, queueName};
}

function dynamicTopic(jms:Durability durability, string subscriberName) returns jms:TopicConfiguration {
    return {topicName: "orders", durability, subscriberName};
}
