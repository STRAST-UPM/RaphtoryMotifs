FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

# raphtory ships a manylinux wheel, so no Rust/build toolchain is required.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY cedar_risk/ ./cedar_risk/
COPY main.py  data_generation.py create_neo4j_graph.py ./

# always generate + load Neo4j, then analyse + submit to Kafka + watch
CMD ["sh", "-c", "python create_neo4j_graph.py && python main.py"]