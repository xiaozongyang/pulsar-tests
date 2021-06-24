# pulsar-tests
This repo is test cases for pulsar, you may run this test cases for survey purpose. Main code is copied from [apache/pulsar](https://github.com/apache/pulsar).

## Quick Start
1. clone this repo `git clone https://github.com/xiaozongyang/pulsar-tests`
2. build runnable jar `cd pulsar-tests && ./gradlew fatJar`
3. run test command which produces 10k messages with 100B body filled with 0-9 `java -jar build/libs/pulsar-tests-0.0.1-all.jar produce -t test-topic`

## Available Commands
- `produce`: produce messages to pulsar, delayed message supported
- `produce-alternately`: produce two kinds of messages, e.g. `a, b, a, b, a,b, ...`
- `consume`: consume messages from specified topic, `readCompacted` option supported
- `ack`: `acknowledge` instead of `consume` message by message id fields, i.e., `ledgerId`, `entryId` and `patitionIndex`