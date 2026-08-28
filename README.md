# GPU Fleet Monitor

A small-scale simulation of a GPU fleet monitoring system, designed to explore how telemetry collection, health rules, issue detection, and alerting could be structured for a large GPU infrastructure.

The project starts with a local simulation and gradually evolves toward a production-oriented monitoring architecture.

## Current Architecture

```text
FakeGPU
   │
   ▼
FakeServer
   │
   ▼
FakeAgent
   │
   │ collect telemetry
   ▼
Telemetry Snapshot
   │
   ▼
FakeObserver
   │
   ▼
TemperatureRule
   │
   ▼
Detected Issues
```

## Current Implementation

### 1. GPU Simulation

Each `FakeGPU` represents a GPU with basic telemetry:

- GPU ID
- Temperature
- Power
- Utilization
- ECC single-bit errors
- ECC double-bit errors
- XID errors
- NVLink errors

GPU utilization can be simulated with:

```text
increase → +10%
decrease → -10%
```

Utilization is bounded between `0%` and `100%`.

Temperature is currently modeled as:

```text
Temperature = 0.5 × Utilization + 40
```

Therefore:

```text
0% utilization   → 40°C
100% utilization → 90°C
```

This is intentionally a simple simulation model rather than a physical GPU thermal model.

### 2. Server Simulation

Each `FakeServer` contains 4 GPUs by default.

The hierarchy is:

```text
Rack
 └── Server
      ├── GPU 0
      ├── GPU 1
      ├── GPU 2
      └── GPU 3
```

Each telemetry record includes:

```text
rack_id
server_id
gpu_id
```

This allows issues to be traced from an individual GPU back to its server and rack.

### 3. Telemetry Collection

`FakeAgent` acts as the local monitoring agent.

Its responsibility is to collect a snapshot of GPU telemetry from the server.

Each snapshot includes a collection timestamp:

```text
collect_time
```

Example telemetry:

```python
{
    "rack_id": "rack-1",
    "server_id": "server-1",
    "gpu_id": 0,
    "gpu_temperature": 72.5,
    "gpu_power": 1400,
    "gpu_utilization": 65,
    "gpu_ecc_single_bit_errors": 0,
    "gpu_ecc_double_bit_errors": 0,
    "gpu_xid_errors": 0,
    "gpu_nvlink_errors": 0,
    "collect_time": ...
}
```

### 4. Issue Detection

`FakeObserver` consumes telemetry snapshots and evaluates monitoring rules.

The first implemented rule is `TemperatureRule`.

Current thresholds:

```text
Temperature <= 85°C       → Normal
85°C < Temperature < 90°C → WARNING
Temperature >= 90°C       → CRITICAL
```

The rule returns:

```text
None
WARNING
CRITICAL
```

The observer converts rule violations into detected issues containing:

```text
rack_id
server_id
gpu_id
issue_type
severity
detect_time
```

## Design Goals

This project is intentionally being developed incrementally.

The goal is not to build a realistic GPU simulator. The goal is to practice designing the components of a production monitoring system and understand the trade-offs involved when scaling from:

```text
1 server
   ↓
multiple servers
   ↓
multiple racks
   ↓
large GPU fleet
```

Important design concerns include:

- Telemetry collection
- Failure detection
- Rule-based health checks
- Rack-level correlation
- Server-level correlation
- Alert deduplication
- Historical telemetry
- Fault aggregation
- Scalability
- Detection latency
- Reliability
- Observability of the monitoring system itself

## Roadmap

### Phase 1 — Simulation

- [x] Simulate GPU state
- [x] Simulate 4 GPUs per server
- [x] Simulate utilization changes
- [x] Generate GPU telemetry snapshots
- [x] Add rack/server/GPU identifiers
- [x] Add collection timestamps

### Phase 2 — Rule-Based Detection

- [x] Temperature monitoring rule
- [x] Warning / critical severity
- [ ] ECC error rule
- [ ] XID error rule
- [ ] NVLink error rule
- [ ] GPU health rule
- [ ] Multiple rules through a common observer interface

### Phase 3 — Historical Telemetry

- [ ] Store telemetry history
- [ ] Query recent GPU state
- [ ] Track state transitions
- [ ] Detect persistent failures
- [ ] Avoid triggering alerts from a single transient sample

### Phase 4 — Fleet-Level Monitoring

- [ ] Simulate multiple servers
- [ ] Simulate multiple racks
- [ ] Detect rack-level failures
- [ ] Aggregate related GPU failures
- [ ] Deduplicate alerts
- [ ] Add fleet-level health state

### Phase 5 — Production-Oriented Architecture

- [ ] Message queue / event pipeline
- [ ] Time-series telemetry storage
- [ ] Alert manager
- [ ] Failure correlation
- [ ] Monitoring service health
- [ ] Horizontal scaling
- [ ] Fault tolerance
- [ ] Detection latency / SLO considerations

## Why This Project?

Large GPU fleets create a different monitoring problem from monitoring a small number of machines.

A single GPU failure is relatively simple to detect.

At fleet scale, the harder problems become:

```text
How do we detect failures quickly?
How do we distinguish GPU failures from server failures?
How do we recognize rack-level failures?
How do we avoid millions of duplicate alerts?
How do we correlate multiple signals into one incident?
How do we make the monitoring system itself reliable?
```

This project explores those problems incrementally, starting from a simple local simulation and evolving toward a scalable monitoring architecture.

## Repository Structure

The project currently starts as a small simulation:

```text
gpu-fleet-monitor/
├── main.py
├── README.md
└── .gitignore
```

As the system grows, components will be separated into modules based on their responsibilities.

## Running Locally

Run the simulation with:

```bash
python3 main.py
```

The program will:

1. Create a simulated server with 4 GPUs.
2. Collect an initial telemetry snapshot.
3. Apply simulated GPU load changes.
4. Collect new telemetry snapshots.
5. Evaluate temperature rules.
6. Print detected issues.

## Development Approach

The project follows an incremental design approach:

```text
Simulation
    ↓
Observation
    ↓
Rules
    ↓
Historical state
    ↓
Aggregation
    ↓
Alerting
    ↓
Fleet-scale architecture
```

Each stage is intentionally kept small so that the design decisions and trade-offs remain visible in the Git history.