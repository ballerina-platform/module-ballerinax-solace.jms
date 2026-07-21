// Copyright (c) 2026 WSO2 LLC. (http://www.wso2.org).
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
