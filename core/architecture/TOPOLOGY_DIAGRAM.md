# Topology Diagram

This diagram shows the currently implemented runtime topology using Docker Compose.

```mermaid
flowchart LR
    SDK[Clients: TypeScript / Python SDK] --> B0[broker-0 id=0 :9092]
    SDK --> B1[broker-1 id=1 :9093]
    SDK --> B2[broker-2 id=2 :9094]

    B0 --- B1
    B1 --- B2
    B0 --- B2

    subgraph NodeInternals["Each Broker Process"]
      N[network handler + protocol v1]
      S[storage: segmented log + indexes]
      C[coordinator: groups + offsets]
      R[replication manager: ISR + HW]
      P[controller boundary: metadata APIs]
    end
```

## Current State Notes

- Multi-broker node creation via Docker is implemented.
- Each broker has independent data volume and broker identity env vars.
- Control-plane metadata store is still in-memory per broker process (durable distributed controller quorum is planned next).
- Broker-to-broker replication protocol (`BROKER_FETCH`, `BROKER_REPLICA_ACK`) and follower worker loop are implemented as runtime scaffolding.
