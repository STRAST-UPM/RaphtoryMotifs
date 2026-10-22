# CEDAR Risk Scoring

A risk-assessment module that assigns an anomaly score (0 – 1) to every
**organization**, **public entity** and **tender** of a CEDAR pilot's
public-procurement graph. It targets two procurement-fraud scenarios:

- **Collusion** — a group of organizations that repeatedly bid together and share
  contracts through coordinated behaviour.
- **Favoritism** — a contracting authority that repeatedly awards its tenders to
  the same group of organizations.

The module is a Kafka service: it listens for analysis requests, scores the
requested pilot's graph, and answers with one response that follows the CEDAR
Kafka message contract.

## How it works

<p align="center">
  <img src="images/pipeline.svg" alt="UPM Raphtory Motifs module: request topic, Neo4j, Raphtory, motifs, scores, results topic" width="95%">
</p>

1. **Consume the request.** The UI publishes a request on the pilot's request
   topic (e.g. `it_analyse`). The module keeps only the requests that ask for its
   indicator, `UPM_RAPHTORY_UC5_AI8`; the topics are shared with other tools.
2. **Read the pilot graph.** Every node and relationship of the pilot's
   knowledge graph is read from Neo4j, through the pilot's HTTP gateway.
3. **Build a Raphtory temporal graph.** The values the pilots store differently
   (booleans, prices, dates) are unified, and every node and edge gets a time.
4. **Extract motifs**, the recurring patterns of each scenario:
   - **Collusion:** groups of 3 organizations that repeatedly bid on the same tenders.
   - **Favoritism:** a public entity repeatedly awarding its tenders to the same group.
5. **Score** every organization, public entity and tender.
6. **Build the response** with the requested entities only, **save it to disk**
   if `OUTPUT_DIR` is set, then **publish** it on the pilot's results topic
   (e.g. `it_results_available`).

Nothing runs between requests: each request is scored on the graph as it is at
that moment.

<table align="center">
  <tr>
    <td align="center" style="padding-right: 50px;">
      <img src="images/motif_1.png" alt="Collusion motif" width="400"><br>
      <b>Collusion</b>
    </td>
    <td align="center" style="padding-left: 50px;">
      <img src="images/motif_5.png" alt="Favoritism motif" width="400"><br>
      <b>Favoritism</b>
    </td>
  </tr>
</table>

### What drives each score

| Node | Scenario | Signals |
|------|----------|---------|
| Organization | Collusion | recurrence of the same bidding group, joint success rate, inflated cover bids, winner rotation, consistent responsible person |
| Public entity | Favoritism | a group repeatedly winning the entity's tenders, high success rate, the group targeting this entity, the same person awards to the same group, restricted competition |
| Tender | Inherited | collusion risk of its winning organization and favoritism risk of its issuing entity (for open tenders, the estimated likelihood a suspicious group wins) |

---

# Configuration

Everything is configured in one `.env` file at the root of the project.
[`.env.example`](.env.example) is a complete template with the production values:

```bash
cp .env.example .env      # then fill in the empty fields
```

## 1. Pilots and their graphs

`PILOTS` lists the pilots this module serves. Each pilot's graph is read through
its HTTP gateway, which needs an API key.

| Variable | Meaning |
|---|---|
| `PILOTS` | pilots to serve, comma or space separated |
| `GRAPH_ENDPOINT` | gateway URL; `{pilot}` is replaced by the pilot's name |
| `GRAPH_API_KEY` | the gateway's API key (sent as `X-API-Key`) |
| `GRAPH_ENDPOINT_<PILOT>`, `GRAPH_API_KEY_<PILOT>` | optional, for one pilot only; they take precedence |

```dotenv
PILOTS=it,sl
GRAPH_ENDPOINT=https://kg-{pilot}.cedar-heu-project.eu/neo4j/query
GRAPH_API_KEY=<the gateway key>

```

A pilot with no API key is skipped at start-up, with a warning in the log.
`python main.py --pilots it` serves only the pilots given on the command line,
whatever `PILOTS` says.

## 2. Kafka connection

The CEDAR cluster uses **SASL_SSL**: TLS with a client certificate signed by the
cluster CA, plus a SASL `PLAIN` username and password. The variables are the ones
of the coordinators' template ([`secure-kafka/README.md`](secure-kafka/README.md));
the credentials come from your partner archive (`secure-kafka/users/upm.7z`),
which extracts to a `upm/` folder holding the three files and `cred.txt`.

| Variable | Meaning |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | the broker, `host:port` |
| `KAFKA_CREDENTIALS_DIR` | the extracted `upm/` folder (`kafka-cedar-ca-1.crt`, `upm.certificate.pem`, `upm.key`) |
| `KAFKA_KEY_PASSWORD` | line 1 of `cred.txt`: the private key passphrase |
| `KAFKA_SASL_USERNAME` | line 2 of `cred.txt` |
| `KAFKA_SASL_PASSWORD` | line 3 of `cred.txt` |

```dotenv
KAFKA_BOOTSTRAP_SERVERS=kafka.cedar-heu-project.eu:8004
KAFKA_CREDENTIALS_DIR=/secure/place/upm          # outside the repository
KAFKA_KEY_PASSWORD=<cred.txt line 1>
KAFKA_SASL_USERNAME=upm
KAFKA_SASL_PASSWORD=<cred.txt line 3>
```

The three files are found in `KAFKA_CREDENTIALS_DIR` under their archive names.
Only if one was renamed or kept elsewhere, point to it directly; each variable
replaces just that one file. Docker ignores them (the container only sees the
mounted folder), so with Docker keep the files in the folder under their names:

| Variable | Default (leave empty to use it) |
|---|---|
| `KAFKA_CA_FILE` | `<KAFKA_CREDENTIALS_DIR>/kafka-cedar-ca-1.crt` |
| `KAFKA_CERT_FILE` | `<KAFKA_CREDENTIALS_DIR>/<KAFKA_SASL_USERNAME>.certificate.pem` |
| `KAFKA_KEY_FILE` | `<KAFKA_CREDENTIALS_DIR>/<KAFKA_SASL_USERNAME>.key` |

## 3. Request and results topics

Each pilot has a **request topic**, where the UI publishes the requests, and a
**results topic**, where the module publishes its responses.

| Variable | Meaning |
|---|---|
| `KAFKA_REQUEST_TOPICS` | the request topics; each one is matched to its pilot by its `<pilot>_` prefix |
| `KAFKA_RESULTS_TOPICS` | the results topics, matched the same way |
| `KAFKA_REQUEST_TOPIC_<PILOT>`, `KAFKA_RESULTS_TOPIC_<PILOT>` | **Optional**, for a topic whose name does not start with `<pilot>_` |

```dotenv
KAFKA_REQUEST_TOPICS=it_analyse,sl_analyse
KAFKA_RESULTS_TOPICS=it_results_available,sl_results_available

# (Optional) a topic with another naming:
KAFKA_REQUEST_TOPIC_IT=cedar.it.requests
```

Without any of them, the contract's names are used: `<pilot>_analyse` and
`<pilot>_results_available`. The topics actually used are logged at start-up:

```text
[pilots]  it: it_analyse -> it_results_available
[pilots]  sl: sl_analyse -> sl_results_available
```

> [!IMPORTANT]
> **A request** is answered only if its `indicators` list contains
> `UPM_RAPHTORY_UC5_AI8`. It scores tenders, organizations and public entities
> (`entityType`: `tender`, `organization`, `publicEntity`).
> `entityId` may be `"all"` (every entity of that type), and
> `{"entityType": "all", "entityId": "all"}` asks for the whole graph:

```json
{
  "issuer": "ui",
  "requestID": "5f1c2e0a-7b8e-4a43-9a51-0b6d3c2f9e11",
  "indicators": ["UPM_RAPHTORY_UC5_AI8"],
  "entities": [{"entityType": "tender", "entityId": "EN_N_101_2025"},
               {"entityType": "organization", "entityId": "all"}],
  "kg": {"versionId": "latest"},
  "accounting": {}, "status": {}
}
```

**The response** carries one indicator per requested entity, with its
`risk_level` and the details behind it:

```json
{
  "requestID": "5f1c2e0a-7b8e-4a43-9a51-0b6d3c2f9e11",
  "responseID": "bcec5730-f420-4fad-adf1-fcb939fef871",
  "toolName": "UPM_RAPHTORY_MOTIFS",
  "analysis": {
    "startTime": 1760536800000, "endTime": 1760536801400,
    "indicators": [{
      "id": "0fba0570-…",
      "indicator_id": "UPM_RAPHTORY_UC5_AI8",
      "entityID": "EN_N_101_2025",
      "risk_level": 0.012,
      "additional_info": [
        {"id": "7ba9a46e-…", "title": "Collusion risk", "type": "numeric", "value": 0.008,
         "risk_level": 0.008, "range": {"from": 0.0, "to": 1.0},
         "description": "Awarded: risk of the winning organization. …"},
        {"id": "d1c485ff-…", "title": "Bidding stage", "type": "text", "value": "awarded"}
      ]
    }]
  },
  "source": {"kg": {"versionId": "latest",
                    "entities": [{"entityType": "tender", "entityId": "EN_N_101_2025"}]}}
}
```


## 4. Consumer group

The module reads the request topics as the Kafka consumer group `KAFKA_GROUP_ID`.
Kafka remembers, per group, which requests were already handled: after a restart
the module answers the requests that arrived while it was down, and never answers
one twice. Keep one fixed name:

```dotenv
KAFKA_GROUP_ID=upm-raphtory
```

## 5. Logger

The log goes to the console and to a file that rotates at midnight:

| Variable | Meaning | Default |
|---|---|---|
| `LOG_LEVEL` | `DEBUG`, `INFO`, `WARNING` or `ERROR` | `INFO` |
| `LOG_FILE` | the log file; `none` = console only | `logs/cedar.log` |
| `LOG_BACKUPS` | days of old log files to keep | unset: every day is kept |

```dotenv
LOG_LEVEL=INFO
LOG_FILE=logs/cedar.log      # rotated into logs/cedar.log.2026-10-15, ...
LOG_BACKUPS=30
```

Every step is logged, and each error with its message and traceback:

```text
2026-10-15 11:40:01 [INFO] upm_raphtory.main: [request] sl 4daba915-… from 'ui': 1 Tender
2026-10-15 11:40:03 [INFO] upm_raphtory.main: [scores]  57 organizations, 1 entities, 36 tenders
2026-10-15 11:40:03 [INFO] upm_raphtory.main: [kafka]   response ae1c6766-… (1 indicators) -> sl_results_available
```

## 6. Saving the responses on disk (optional)

`OUTPUT_DIR` is the folder where each response is saved as JSON, **before** it is
published, so it is kept even if Kafka is unreachable or refuses it. When
`OUTPUT_DIR` is not set (or empty), responses are **not saved**: they are only
published to the Kafka broker.

| Variable | Meaning | Default |
|---|---|---|
| `OUTPUT_DIR` | folder for the saved responses; start the path with `./` or `/` | unset: not saved |

```dotenv
OUTPUT_DIR=./outputs
```

```text
outputs/sl_latest.json                              the last response of the pilot
outputs/sl/20261015T094003Z_4daba915-….json          one file per response
```
---

# Running the module

Once `.env` is configured, one command builds and starts the module (the
installed package, from `pyproject.toml`):

```bash
make run         # build and start the module
make stop        # stop it
make restart     # after a change in the code or in .env
make logs        # follow the log (also in logs/cedar.log)
make status      # is it running?
make help        # every command of this branch
```

The container reads the same `.env`, uses the host's network (so
`KAFKA_BOOTSTRAP_SERVERS` means the same as above), mounts `KAFKA_CREDENTIALS_DIR`
read-only, and keeps `OUTPUT_DIR` (if set) and `logs/` on the host. It restarts
by itself after a crash or a reboot.

**From the source** (for development), instead of the package. Both use the
container name `cedar-risk`, so only one runs at a time: `make run` and
`make source-run` first remove the existing one, and `make stop` stops either.

```bash
make source-run                  # the source, in Docker
make python                      # python main.py: every pilot in PILOTS
make python ARGS="--pilots it"   # only the Italian pilot
```

---

# Test scenarios

This branch (`test`) holds everything needed to test the module without the
CEDAR cluster or the pilot graphs. `main` holds the same code, documented for
production only.

| # | Scenario | What it shows | Needs |
|---|---|---|---|
| 1 | [Request → response on a local broker](#1-request--response-on-a-local-kafka-broker) | the full Kafka flow, as in production | local broker, pilot graphs |
| 2 | [Which requests are answered](#2-which-requests-are-answered) | one entity, all of a type, the whole graph, another tool's request | scenario 1 |
| 3 | [Saving on disk or only publishing](#3-saving-on-disk-or-only-publishing) | `OUTPUT_DIR` set / not set, `DRY_RUN` | scenario 1 |
| 4 | [The broker on another machine](#4-the-broker-on-another-machine) | the same setup on a server (e.g. `echo`) | a second machine |
| 5 | [Synthetic anomaly scenarios](#5-synthetic-anomaly-scenarios) | the scoring on planted collusion and favoritism cases | local Neo4j only |

## 1. Request → response on a local Kafka broker

`kafka/` holds a local broker that replicates the CEDAR cluster: same port (8004),
same security (SASL_SSL with a required client certificate), same credential
files, and the four pilot topics. Its credentials come from a **local** CA and are
never valid on the real cluster.

**Create the local credentials** (once). `kafka/secrets/local/upm/` gets the same
files as the production archive:

```bash
make kafka-credentials
```

**Point `.env` at the local broker** (the three secrets are in
`kafka/secrets/local/upm/cred.txt`, lines 1 – 3):

```dotenv
KAFKA_BOOTSTRAP_SERVERS=localhost:8004
KAFKA_CREDENTIALS_DIR=./kafka/secrets/local/upm
KAFKA_KEY_PASSWORD=<cred.txt line 1>
KAFKA_SASL_USERNAME=upm
KAFKA_SASL_PASSWORD=<cred.txt line 3>
```

**Start the broker, then the module:**

```bash
make kafka-up            # broker on localhost:8004 + the topics
make run                 # the module (or: make python ARGS="--pilots sl")
make logs                # ... [kafka]   waiting for requests on it_analyse, sl_analyse
```

**Send a request** as the UI would; `send_request.py` waits for the answer:

```bash
make request ARGS="--pilots sl --tender EN_N_101_2025"
```

```text
[sent]    sl: request 4daba915-… -> sl_analyse  (1 entities)
[result]  sl: response ae1c6766-… after 1.4s, 1 indicators (1 UPM_RAPHTORY_UC5_AI8)
```

**Read the results topic** directly, and **stop** everything:

```bash
make results ARGS=sl_results_available
make stop
make kafka-down
```

After `make kafka-down` the local broker forgets everything (topics, offsets).
Restart a module that kept running through it (`make restart`): its connection
belongs to the old broker.

## 2. Which requests are answered

With scenario 1 running:

```bash
make request ARGS="--pilots sl --tender EN_N_101_2025"          # one tender
make request ARGS="--pilots it --organization all"              # every organization
make request ARGS="--pilots sl --public-entity all"             # every public entity
make request ARGS="--pilots sl --everything --print"            # the whole graph, full JSON
make request ARGS="--pilots sl --tender UNKNOWN_ID"             # answered "Analysis Failed"
make request ARGS="--pilots sl --indicators OTHER_TOOL_ID --wait 10"   # not ours: no response
```

## 3. Saving on disk or only publishing

| `.env` | Result |
|---|---|
| `OUTPUT_DIR=./outputs` | each response saved in `outputs/<pilot>/`, then published |
| no `OUTPUT_DIR` | only published; the log says `responses not saved on disk` |
| `DRY_RUN=1` (+ `OUTPUT_DIR`) | answered and saved, **not** published |

Change `.env`, `make restart`, then send a request (scenario 2).

## 4. The broker on another machine

The same files run the broker on a server, e.g. `echo`. On that machine, set
`KAFKA_BOOTSTRAP_SERVERS=echo:8004` in its `.env`, then:

```bash
make kafka-credentials   # adds "echo" to the broker certificate
make kafka-up            # the broker advertises echo:8004
```

On the machines that use it, set `KAFKA_BOOTSTRAP_SERVERS=echo:8004` too, with the
same `kafka/secrets/local/upm/` credentials (copied from the server).

## 5. Synthetic anomaly scenarios

`synthetic_anomalies/` builds a local Neo4j graph with a synthetic market and
**planted** anomalies, and scores it **without Kafka**:

- **collusion:** a full ring (same responsible persons), a partial ring, a
  near-cartel, coincidental repetitions, and groups at different distances from
  the scenario (seen once, always losing, half-way);
- **favoritism:** an entity favouring a group through the same official, and a
  ladder of public entities captured to different degrees;
- **edge cases:** new and small organizations, new and small public entities.

It needs only `SYNTHETIC_GRAPH_API_KEY` in `.env` (the key of the local Neo4j
gateway, any value you choose; `SYNTHETIC_GRAPH_ENDPOINT` defaults to
`http://localhost:8000/neo4j/query`).

```bash
make synthetic-up        # Neo4j + gateway, then loads the synthetic graph
make synthetic-test      # scores it
make synthetic-down
```

`outputs/synthetic/` then holds the response (same template as for the pilots)
and `ground_truth.json`, the planted scenarios to compare it with.

---

# Contact

For any issue with this module, contact:

| Name | Email |
|---|---|
| Félix Cuadrado | [felix.cuadrado@upm.es](mailto:felix.cuadrado@upm.es) |
| José Miguel Blanco | [josemiguel.blanco@upm.es](mailto:josemiguel.blanco@upm.es) |
| Wassim Sliti | [wassim.sliti@upm.es](mailto:wassim.sliti@upm.es) |

