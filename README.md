# kafka-rebalance-viz

> CLI tool to visualize and analyze Kafka consumer group rebalancing events from logs

---

## Installation

```bash
pip install kafka-rebalance-viz
```

Or install from source:

```bash
git clone https://github.com/youruser/kafka-rebalance-viz.git
cd kafka-rebalance-viz
pip install .
```

---

## Usage

Point the tool at a Kafka broker or consumer log file to get a visual breakdown of rebalancing events:

```bash
# Analyze a local log file
kafka-rebalance-viz analyze --file /var/log/kafka/consumer.log

# Filter by consumer group and output a timeline
kafka-rebalance-viz analyze --file consumer.log --group my-consumer-group --format timeline

# Watch live logs from stdin
tail -f consumer.log | kafka-rebalance-viz analyze --stdin
```

**Example output:**

```
[2024-03-01 12:00:01]  Rebalance START  →  group: my-consumer-group  members: 4
[2024-03-01 12:00:03]  Partition assign →  consumer-1: [0,1]  consumer-2: [2,3]
[2024-03-01 12:00:03]  Rebalance END    →  duration: 2.1s  partitions: 4
```

### Options

| Flag | Description |
|------|-------------|
| `--file` | Path to the log file |
| `--group` | Filter by consumer group name |
| `--format` | Output format: `timeline`, `summary`, `json` |
| `--stdin` | Read log lines from standard input |

---

## License

This project is licensed under the [MIT License](LICENSE).