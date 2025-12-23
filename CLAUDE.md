# Active-Active Counter Store — v1 Specification

## 1. Goal & Scope

Build a **high-performance, in-memory, active-active counter database** optimized for:

- Local low-latency increments
- Multi-region correctness
- Eventual convergence without coordination
- Operational simplicity

This system is **not** a general-purpose database.

---

## 2. Core Guarantees (Must Have)

1. No lost increments, even with concurrent writes in multiple regions
2. Local writes do not block on cross-region communication
3. Deterministic convergence across all replicas
4. Idempotent replication
5. Availability during partitions (AP system)

---

## 3. Data Model

### 3.1 Counter Type

- G-Counter (grow-only)
- Optional future: PN-Counter

### 3.2 Internal State

```
key → { replicaId → uint64 }
```

### 3.3 Value Computation

```
value(key) = sum(all replica components)
```

### 3.4 Merge Rule

```
component = max(local, remote)
```

---

## 4. API (Minimal v1)

### Required

- INCRBY key amount
- GET key
- MGET keys[]

---

## 5. Time Windows & Resets

### Required Approach

**Time-bucketed keys**

Examples:

```
requests:{customer}:{YYYYMMDDHHmm}
errors:{service}:{YYYYMMDD}
```

### Explicit Non-Goal

- No “reset to zero” operation

---

## 6. Replication Model

### Replication Unit

Delta per (key, replicaId)

Payload:

```
{
  key,
  originReplicaId,
  componentValue
}
```

### Transport

- Long-lived, bidirectional streaming
- Authenticated and encrypted (mTLS recommended)
- gRPC streaming over HTTP/2 recommended

---

## 7. Replication Behavior

- Idempotent apply
- Out-of-order safe
- Duplicate-safe
- Delta compaction strongly recommended
- Periodic anti-entropy optional

---

## 8. Topology

- Hub-and-spoke or small mesh recommended

---

## 9. Storage

- In-memory hashmap
- Sparse per-key replica maps

---

## 10. Client Connections

- Persistent request/response
- No client Pub/Sub in v1

---

## 11. Consistency Model

- Eventually consistent
- Monotonic growth for G-Counters
- Reads reflect local knowledge only

---

## 12. Identity

- Stable replicaId required
- Persisted across restarts

---

## 13. Explicit Non-Goals

- Strong consistency
- Transactions
- Arbitrary KV storage
- Client Pub/Sub

---

## 14. Positioning

> A globally correct, edge-native counter store with local writes and deterministic convergence.
