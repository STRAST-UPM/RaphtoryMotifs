# Two ways to run the tool, pick one with --target:
#
#   source  (default)  copy the code, run `python main.py`
#       docker build -t cedar-risk .
#       docker build -t cedar-risk --target source .
#
#   package            build the wheel from pyproject.toml, install it, run `cedar-risk`
#       docker build -t cedar-risk:package --target package .
#
# Run (either image), with the settings from .env and the outputs/logs kept on the host:
#   docker run --rm --env-file .env -v ./outputs:/app/outputs -v ./logs:/app/logs cedar-risk
#   docker run ... cedar-risk --pilots it sl          extra arguments go to main.py

# ---- shared base -------------------------------------------------------------
FROM python:3.12-slim AS base
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

# ---- builder: produce the wheel ---------------------------------------------
FROM base AS builder
WORKDIR /src
RUN pip install build
COPY pyproject.toml README.md main.py ./
COPY cedar_risk/ ./cedar_risk/
RUN python -m build --wheel --outdir /dist

# ---- package: the installed wheel -------------------------------------------
FROM base AS package
# raphtory ships a manylinux wheel, so no Rust/build toolchain is required.
COPY --from=builder /dist/*.whl /tmp/
RUN pip install /tmp/*.whl && rm /tmp/*.whl
ENTRYPOINT ["cedar-risk"]

# ---- source: plain `python main.py` (default target, last stage) ------------
FROM base AS source
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY cedar_risk/ ./cedar_risk/
COPY main.py ./
ENTRYPOINT ["python", "main.py"]
