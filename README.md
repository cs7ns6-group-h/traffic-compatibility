# traffic-compatibility - CS7NS6 Group H

**Owner:** Marta Fraioli

Traffic/Compatibility Service - subscribes to journey booking requests via Kafka, computes routes using OpenStreetMap data, overlays real-time traffic conditions from traffic authorities, and publishes `journey.accepted` or `journey.rejected` decisions.

## Structure

```
traffic-compatibility/
├── src/
│   └── traffic_compatibility/
│       ├── main.py            # FastAPI app + Traffic Authority API
│       ├── kafka_consumer.py  # Subscribes to journey.requested
│       ├── kafka_producer.py  # Publishes journey.accepted/rejected
│       ├── cassandra_client.py
│       └── router.py          # Route computation + compatibility check
├── tests/
│   └── test_router.py
├── .github/workflows/
│   └── docker-publish.yml     # Builds and pushes image to ghcr.io
├── Dockerfile
└── requirements.txt
```

## Running locally

```bash
pip install -r requirements.txt
uvicorn src.traffic_compatibility.main:app --reload
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET | `/status` | Active closures and monitored segments |
| POST | `/authority/closure` | Submit road closure (Traffic Authority) |
| DELETE | `/authority/closure/{segment_id}` | Remove road closure (Traffic Authority) |
| GET | `/authority/closures` | List all active road closures |
| POST | `/authority/traffic` | Submit congestion update (Traffic Authority) |
| POST | `/authority/emergency` | Issue emergency override (Traffic Authority) |
| GET | `/authority/segments/{segment_id}/journeys` | Get active journeys on a segment |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REGION` | `eu` | Region identifier (eu / us / apac) |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka-eu:9092` | Kafka broker address |
| `CASSANDRA_HOSTS` | `cassandra-eu` | Cassandra contact points |
| `CASSANDRA_DC` | `eu` | Cassandra local datacenter |

## Docker image

Published automatically to `ghcr.io/cs7ns6-group-h/traffic-compatibility:latest` on every push to `main`.