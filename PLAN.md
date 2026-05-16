# Production Plan: real-time-event-streaming

This document is the execution roadmap for evolving `real-time-event-streaming` into a production-grade distributed event streaming platform in Go.

## 1. Objectives

- Build a durable, scalable, fault-tolerant streaming system with predictable latency.
- Support high-throughput producers and consumers with strong ordering guarantees per partition.
- Provide reliable consumer group coordination and offset management.
- Operate safely in production with observability, security, and SRE-ready workflows.

## 2. Non-Goals (Initial Releases)

- Exactly-once semantics across all sinks.
- Multi-region active-active replication in v1.
- Arbitrary server-side stream processing in broker runtime.

## 3. Product Requirements (Target v1)

- Topics with configurable partition count and replication factor.
- Producer acks (`acks=0|1|all`) and idempotent producer mode.
- Consumer groups with automatic rebalancing and durable commits.
- Retention policies: time-based + size-based.
- Broker failover via controller/metadata quorum.
- Rolling upgrade support and backward-compatible protocol evolution.
- Production telemetry (metrics/logs/traces), alerting, and admin tooling.

## 4. Architecture Targets

- **Control Plane**
  - Cluster metadata (topics, partitions, leaders, ISR, ACLs, configs).
  - Consensus-backed controller service for leader election and metadata updates.
- **Data Plane**
  - Brokers host partition replicas and serve produce/fetch APIs.
  - Log segments with index files and periodic fsync policies.
  - Replication pipeline with ISR management and high-watermark tracking.
- **Client Plane**
  - Go client SDK for producer/consumer/admin operations.
  - Metadata discovery and partition leader routing.

## 5. Phased Roadmap

## Phase 0: Stabilize Existing Prototype (1-2 weeks)

- ~~Harden protocol parsing and error handling.~~
- ~~Add explicit request/response envelopes with correlation IDs.~~
- ~~Define versioned protocol compatibility and error code registry.~~
- ~~Centralize configuration (ports, data dir, partitions, flush policy).~~
- ~~Introduce structured logging.~~
- ~~Add unit tests for storage, group balancing, offset handling, protocol parser.~~

**Exit Criteria**
- Invalid inputs do not crash broker.
- Reproducible local runs with config file/env overrides.
- Baseline test coverage for all current modules.

## Phase 1: Storage Engine v1 (2-4 weeks)

- ~~Replace single log files with segmented append-only logs.~~
- ~~Add offset and timestamp indexes per segment.~~
- ~~Implement retention + segment compaction primitives.~~
- ~~Add startup recovery with checksum validation.~~
- ~~Define durability knobs (`flush.interval`, `flush.bytes`, `fsync.mode`).~~

**Exit Criteria**
- Restart recovery proven by tests.
- Configurable retention verified under load.
- No data loss under clean restart scenarios.

## Phase 2: Replication and Partition Leadership (3-5 weeks)

- Implement follower fetch/replication protocol.
- Track ISR and high watermark per partition.
- Support leader/follower role transitions.
- Implement produce ack modes (`acks=1`, `acks=all`).
- Add replica lag monitoring and ISR shrink/expand logic.
- Add under-replicated partition detection and alert hooks.

**Exit Criteria**
- Replicated writes survive single broker failure.
- Consumers only read committed data (up to high watermark).
- Deterministic partition leadership transitions in tests.

## Phase 3: Consumer Group Coordination v2 (2-4 weeks)

- Heartbeats + session timeouts.
- Join/Sync/Rebalance protocol states.
- Assignors: range and round-robin.
- Durable group metadata and member generation IDs.
- Offset commit validation against member generation.
- Enforce unique member identity per group and reject duplicate joins safely.

**Exit Criteria**
- Rebalance correctness under member joins/leaves/crashes.
- No duplicate partition ownership in steady state.
- Offset commits remain valid through rebalances.

## Phase 4: Metadata Quorum and Cluster Control Plane (4-6 weeks)

- Build controller service with Raft-based metadata store.
- Migrate topic/partition metadata from local state to quorum.
- Implement leader election for partitions.
- Add broker registration, health, and fencing.
- Add cluster metadata and health admin APIs (topic lifecycle + broker state).

**Exit Criteria**
- Metadata survives controller failover.
- Cluster can recover leadership after node restart.
- Admin operations (create topic, alter configs) are consistent.

## Phase 5: Security and Multi-Tenancy (2-4 weeks)

- TLS for client-broker and broker-broker traffic.
- SASL auth (start with SCRAM).
- ACL model for topic/group/admin actions.
- Quotas and rate limits per client/principal.

**Exit Criteria**
- Unauthorized produce/consume/admin requests denied.
- Encrypted traffic verified end-to-end.
- Quota enforcement validated by load tests.

## Phase 6: Observability and Operations (2-3 weeks)

- Prometheus metrics and OpenTelemetry traces.
- Structured logs with request correlation.
- Alerting dashboards (latency, ISR churn, under-replicated partitions, disk usage).
- CLI/admin APIs for cluster and topic management.
- Backup/restore runbook and disaster recovery drills.

**Exit Criteria**
- SLO dashboards and alerts in place.
- Common incident workflows documented and tested.
- On-call playbook available for top failure modes.

## Phase 7: Performance and Scale Validation (ongoing)

- Benchmark harness (produce/fetch throughput, p99 latency).
- Soak tests (24h+), chaos tests, and fault injection.
- CPU/memory/profile-guided optimization.
- Network batching, compression, zero-copy improvements.

**Exit Criteria**
- Defined throughput and p99 latency targets achieved.
- Stability under sustained load and fault conditions.

## 6. Cross-Cutting Engineering Standards

- API compatibility policy and versioned protocol evolution.
- Strict CI gates: `go test`, race detector, lint, static analysis.
- Contract tests for protocol and interoperability.
- Backward-compatible storage migration strategy.
- Feature flags for risky rollouts.

## 7. Testing Strategy

- Unit tests: storage, protocol, coordinator, replication logic.
- Integration tests: multi-broker cluster behavior.
- End-to-end tests: produce -> replicate -> consume -> commit.
- Fault tests: broker crash, slow disk, network partition.
- Upgrade tests: rolling restart across versions.

## 8. Release Milestones

- **M1 (Prototype Hardening):** End of Phase 0-1
- **M2 (HA Data Plane):** End of Phase 2-3
- **M3 (Clustered Control Plane):** End of Phase 4
- **M4 (Prod Readiness):** End of Phase 5-6
- **M5 (Scale Validation):** Phase 7 targets met

## 9. Immediate Backlog (Next 2 Weeks)

1. Refactor storage into segmented logs.
2. Add GitHub Actions CI with lint + race + tests.
3. Introduce Prometheus metrics skeleton.
4. Scaffold `internal/replication` with interfaces and integration test harness.
5. Add consumer-group generation ID and duplicate-member guard in coordinator.

## 10. Risks and Mitigations

- **Risk:** Data corruption on unclean shutdown.
  - **Mitigation:** Checksums, WAL discipline, recovery replay tests.
- **Risk:** Rebalance storms under churn.
  - **Mitigation:** Heartbeat tuning, cooperative rebalance design.
- **Risk:** Metadata inconsistency in failover.
  - **Mitigation:** Consensus-backed control plane before HA claims.
- **Risk:** Performance regressions while adding safety.
  - **Mitigation:** Continuous benchmarks and regression thresholds.

## 11. Definition of Production-Grade (Go/No-Go)

- Proven durability and replication guarantees in automated tests.
- Fault tolerance with documented RTO/RPO for supported failures.
- Security baseline (TLS + auth + ACL) enabled by default.
- SLO-backed observability with actionable alerts.
- Runbooks and upgrade procedures validated in staging.
- Performance targets met in repeatable benchmark environments.
