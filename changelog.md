# Changelog

This file contains all the notable changes done to the Ballerina `solace.jms` package through the releases.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `jms:MessageProducer` client to send messages to a Solace queue or topic
- `jms:MessageConsumer` client to receive messages from a Solace queue or topic
- `jms:Listener` to receive messages from a Solace queue or topic via a Ballerina service
- `jms:Caller` to acknowledge messages and manage transactions within a service
- Support for basic, Kerberos, and OAuth2 authentication, and TLS secured connections
- Support for setting a per-message time-to-live via `Message.timeToLive`
- Support for configuring guaranteed-delivery flow control via `transportWindowSize`, `ackThreshold`, and `ackTimer`
