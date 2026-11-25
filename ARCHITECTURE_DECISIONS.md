# Architecture Decisions

## Milestone 1: In-memory producer retries

The lab retries a retriable send failure for match A up to three times, using a
Vert.x timer between attempts. The next update in that match's chain waits for
the previous send to succeed. During a broker outage, HTTP remained responsive;
after the broker returned, both test updates arrived in order.

This is not durable recovery:

- Pending updates are held only in memory and can be lost if the process stops.
- Once retries are exhausted, the chain fails and logs the error; it does not
  persist or replay the update.
- A failed send can have an uncertain outcome. A new application-level send
  may duplicate an update even with Kafka producer idempotence enabled.
- The example sequences only its explicitly chained sends; it does not
  coordinate independently started chains for the same match.

The single-broker lab also has no replicated copy of topic data. Durable
recovery and duplicate handling require a separate design, such as a replayable
source or a transactional outbox with consumer-side deduplication.
