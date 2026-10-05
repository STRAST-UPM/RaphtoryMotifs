import os
import json
from pathlib import Path

import requests
from dotenv import load_dotenv

# ============================================================
# Configuration
# ============================================================

PILOTS = ["it", "ua", "sl"]

OUTPUT_DIR = Path("cedar_export")

load_dotenv()

API_KEY = os.getenv("GRAPH_API_KEY")


# ============================================================
# Cypher queries
# ============================================================

READ_NODES = """
MATCH (n)
RETURN labels(n)[0] AS label,
       n.id AS id,
       properties(n) AS props
"""

READ_RELS = """
MATCH (a)-[r]->(b)
RETURN type(r) AS rel,
       a.id AS src,
       b.id AS dst,
       properties(r) AS props
"""


# ============================================================
# Gateway helpers
# ============================================================

def pilot_setting(name, pilot, default=None):
    """
    Read pilot-specific environment variable first.

    Example:
        GRAPH_ENDPOINT_IT
        GRAPH_ENDPOINT_UA
        GRAPH_ENDPOINT_SL

    Then fall back to the generic variable.
    """
    return (
        os.getenv(f"{name}_{pilot.upper()}")
        or os.getenv(name)
        or default
    )


def pilot_endpoint(pilot):
    """
    Resolve the HTTP Neo4j gateway endpoint for a pilot.
    """

    endpoint = pilot_setting(
        "GRAPH_ENDPOINT",
        pilot,
        f"https://kg-{pilot}.cedar-heu-project.eu/neo4j/query",
    )

    return endpoint.format(pilot=pilot)


def run_cypher(query, endpoint, api_key, parameters=None):
    """
    Execute a Cypher query through the HTTP gateway.

    The gateway returns either:

        {"result": [...]}

    or directly:

        [...]
    """

    response = requests.post(
        endpoint,
        headers={
            "X-API-Key": api_key,
            "Content-Type": "application/json",
        },
        json={
            "query": query,
            "parameters": parameters or {},
        },
        timeout=60,
    )

    response.raise_for_status()

    data = response.json()

    if isinstance(data, dict) and "result" in data:
        return data["result"]

    return data


def read_from_neo4j(endpoint, api_key):
    """
    Read the complete graph from the remote Neo4j gateway.

    Returns:
        nodes
        rels
    """

    nodes = run_cypher(
        READ_NODES,
        endpoint,
        api_key,
    )

    rels = run_cypher(
        READ_RELS,
        endpoint,
        api_key,
    )

    return nodes, rels


# ============================================================
# Cypher serialization
# ============================================================

def cypher_identifier(value):
    """
    Escape a Cypher identifier such as a label or relationship type.
    """

    value = str(value)

    return "`" + value.replace("`", "``") + "`"


def cypher_value(value):
    """
    Convert a Python value into a Cypher literal.
    """

    if value is None:
        return "null"

    if isinstance(value, bool):
        return "true" if value else "false"

    if isinstance(value, (int, float)):
        return json.dumps(
            value,
            allow_nan=False,
        )

    if isinstance(value, str):
        return json.dumps(
            value,
            ensure_ascii=False,
        )

    if isinstance(value, list):
        return "[" + ", ".join(
            cypher_value(item)
            for item in value
        ) + "]"

    if isinstance(value, dict):
        # Neo4j properties normally do not contain maps.
        # Keep unexpected dictionaries as JSON strings.
        return json.dumps(
            json.dumps(
                value,
                ensure_ascii=False,
            ),
            ensure_ascii=False,
        )

    return json.dumps(
        str(value),
        ensure_ascii=False,
    )


def cypher_map(properties):
    """
    Convert a Python dictionary into a Cypher property map.

    Example:

        {"id": "123", "name": "ABC"}

    becomes:

        {`id`: "123", `name`: "ABC"}
    """

    if not properties:
        return "{}"

    parts = []

    for key, value in properties.items():

        # Neo4j does not store null properties.
        if value is None:
            continue

        parts.append(
            f"{cypher_identifier(key)}: "
            f"{cypher_value(value)}"
        )

    if not parts:
        return "{}"

    return "{ " + ", ".join(parts) + " }"


# ============================================================
# Export one pilot
# ============================================================

def export_pilot(pilot, api_key):
    """
    Download one pilot graph and write it as a Cypher file.
    """

    print()
    print("=" * 60)
    print(f"Downloading pilot: {pilot}")
    print("=" * 60)

    endpoint = pilot_endpoint(pilot)

    print(f"Endpoint: {endpoint}")

    # --------------------------------------------------------
    # Download graph
    # --------------------------------------------------------

    print()
    print("Reading nodes...")

    nodes = run_cypher(
        READ_NODES,
        endpoint,
        api_key,
    )

    print(f"Nodes received: {len(nodes):,}")

    print()
    print("Reading relationships...")

    rels = run_cypher(
        READ_RELS,
        endpoint,
        api_key,
    )

    print(f"Relationships received: {len(rels):,}")

    # --------------------------------------------------------
    # Validate nodes
    # --------------------------------------------------------

    valid_nodes = []

    for node in nodes:

        node_id = node.get("id")

        if node_id is None:
            print(
                "WARNING: skipping node without id:",
                node,
            )
            continue

        valid_nodes.append(node)

    # IDs available locally.
    node_ids = {
        str(node["id"])
        for node in valid_nodes
    }

    # --------------------------------------------------------
    # Validate relationships
    # --------------------------------------------------------

    valid_rels = []

    for rel in rels:

        src = rel.get("src")
        dst = rel.get("dst")

        if src is None or dst is None:
            print(
                "WARNING: skipping relationship without endpoint:",
                rel,
            )
            continue

        if str(src) not in node_ids:
            print(
                "WARNING: relationship source not found:",
                src,
            )
            continue

        if str(dst) not in node_ids:
            print(
                "WARNING: relationship destination not found:",
                dst,
            )
            continue

        valid_rels.append(rel)

    # --------------------------------------------------------
    # Output path
    # --------------------------------------------------------

    pilot_dir = OUTPUT_DIR / pilot

    pilot_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_file = pilot_dir / "graph.cypher"

    print()
    print(f"Writing: {output_file}")

    # --------------------------------------------------------
    # Write Cypher
    # --------------------------------------------------------

    with output_file.open(
        "w",
        encoding="utf-8",
    ) as file:

        file.write(
            "// ====================================================\n"
        )

        file.write(
            f"// CEDAR pilot: {pilot}\n"
        )

        file.write(
            "// Generated from the HTTP Neo4j gateway\n"
        )

        file.write(
            "// Read-only export of the remote graph\n"
        )

        file.write(
            "// ====================================================\n\n"
        )

        # ----------------------------------------------------
        # Nodes
        # ----------------------------------------------------

        file.write(
            "// ====================================================\n"
        )

        file.write(
            "// NODES\n"
        )

        file.write(
            "// ====================================================\n\n"
        )

        for node in valid_nodes:

            node_id = node["id"]

            label = node.get(
                "label",
                "Node",
            )

            properties = dict(
                node.get("props") or {}
            )

            # Make sure the ID exists because relationships
            # will use it to reconnect nodes.
            properties["id"] = node_id

            file.write(
                "CREATE (n:"
                + cypher_identifier(label)
                + " "
                + cypher_map(properties)
                + ");\n"
            )

        # ----------------------------------------------------
        # Relationships
        # ----------------------------------------------------

        file.write("\n")

        file.write(
            "// ====================================================\n"
        )

        file.write(
            "// RELATIONSHIPS\n"
        )

        file.write(
            "// ====================================================\n\n"
        )

        for rel in valid_rels:

            src = rel["src"]
            dst = rel["dst"]

            rel_type = rel.get(
                "rel",
                "RELATED_TO",
            )

            properties = rel.get(
                "props"
            ) or {}

            file.write(
                "MATCH (a {id: "
                + cypher_value(src)
                + "}), "
                "(b {id: "
                + cypher_value(dst)
                + "})\n"
            )

            file.write(
                "CREATE (a)-[:"
                + cypher_identifier(rel_type)
                + " "
                + cypher_map(properties)
                + "]->(b);\n"
            )

        file.write("\n")

        file.write(
            "// ====================================================\n"
        )

        file.write(
            "// END OF EXPORT\n"
        )

        file.write(
            "// ====================================================\n"
        )

    # --------------------------------------------------------
    # Summary
    # --------------------------------------------------------

    print()
    print("Export completed:")
    print(f"  Pilot          : {pilot}")
    print(f"  Nodes          : {len(valid_nodes):,}")
    print(f"  Relationships  : {len(valid_rels):,}")
    print(f"  Output         : {output_file}")


# ============================================================
# Main
# ============================================================

def main():

    if not API_KEY:
        raise RuntimeError(
            "GRAPH_API_KEY environment variable is not set.\n\n"
            "Set it with:\n"
            "    export GRAPH_API_KEY='YOUR_API_KEY'\n"
        )

    print()
    print("CEDAR Neo4j Graph Downloader")
    print("============================")
    print()

    for pilot in PILOTS:

        export_pilot(
            pilot,
            API_KEY,
        )

    print()
    print("=" * 60)
    print("ALL PILOTS EXPORTED")
    print("=" * 60)
    print()
    print("Output directory:")
    print(OUTPUT_DIR.resolve())
    print()
    print("Generated files:")

    for pilot in PILOTS:
        print(
            f"  {OUTPUT_DIR / pilot / 'graph.cypher'}"
        )


if __name__ == "__main__":
    main()