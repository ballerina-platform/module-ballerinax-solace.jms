import ballerinax/solace.jms as jms;

jms:QueueConfiguration defaultDurableQueue = {};
jms:QueueConfiguration durableQueue = {durability: jms:DURABLE};
jms:QueueConfiguration emptyDurableQueue = {queueName: ""};
jms:QueueConfiguration namedTemporaryQueue = {queueName: "orders", durability: jms:TEMPORARY};

jms:TopicConfiguration durableTopic = {topicName: "orders", durability: jms:DURABLE};
jms:TopicConfiguration emptyDurableTopic = {
    topicName: "orders",
    durability: jms:DURABLE,
    subscriberName: ""
};

jms:Service durableTopicService = @jms:ServiceConfig {
    topicName: "orders",
    durability: jms:DURABLE
} service object {
    remote function onMessage(jms:Message message) returns error? {}
};

jms:QueueConfiguration temporaryQueue = {durability: jms:TEMPORARY};
jms:QueueConfiguration emptyTemporaryQueue = {queueName: "", durability: jms:TEMPORARY};
jms:TopicConfiguration temporaryTopic = {topicName: "events"};
jms:QueueConfiguration namedQueue = {queueName: "orders"};
jms:TopicConfiguration namedDurableTopic = {
    topicName: "orders",
    durability: jms:DURABLE,
    subscriberName: "orders-subscriber"
};
