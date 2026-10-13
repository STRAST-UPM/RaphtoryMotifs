#!/usr/bin/env bash
# Local replica of the CEDAR production Kafka credentials (see secure-kafka/README.md).
#
# Production: SASL_SSL = TLS with a client certificate signed by the project CA,
# plus SASL PLAIN user/password; each partner gets a <user>/ folder holding
#     kafka-cedar-ca-1.crt  <user>.certificate.pem  <user>.key  cred.txt
# This script builds the same thing with a LOCAL CA (nothing here is valid on
# the production cluster), plus what the local broker needs:
#
#     kafka/secrets/local/
#       ca/                      local CA (key + kafka-cedar-ca-1.crt)
#       broker/                  mounted into the broker as /etc/kafka/secrets
#         broker.pem             broker key + certificate (SAN: kafka, localhost)
#         kafka-cedar-ca-1.crt   CA the broker trusts client certificates from
#         kafka_server_jaas.conf SASL PLAIN users (admin + every user below)
#         admin.pem              admin client key + certificate (kafka-init)
#         admin-client.properties
#       <user>/                  same layout as the production archive
#         kafka-cedar-ca-1.crt   <user>.certificate.pem   <user>.key   cred.txt
#
# cred.txt, as in production: line 1 key passphrase, line 2 SASL username,
# line 3 SASL password.
#
#   ./kafka/make-local-credentials.sh              # user "upm"
#   ./kafka/make-local-credentials.sh upm ui       # several users
#   ./kafka/make-local-credentials.sh --force upm  # start over with a new CA
#
# Broker on another machine: its certificate must name the host clients connect
# to (they check it, as in production). Add the names / IPs with --broker-host
# (repeatable) or KAFKA_BROKER_HOSTS; only the broker certificate is reissued,
# the CA and the users' credentials stay valid:
#   ./kafka/make-local-credentials.sh --broker-host echo --broker-host echo.dit.upm.es
#   KAFKA_BROKER_HOSTS="echo echo.dit.upm.es 138.4.11.230" ./kafka/make-local-credentials.sh
# and start that broker with KAFKA_ADVERTISED_HOST=<one of them> (docker-compose.yml).
#
# Existing files are kept, so re-running only adds what is missing; restart
# the broker afterwards (docker compose --profile local-kafka up -d --force-recreate).

set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)/secrets/local"
DAYS=365
SUBJ="/C=EU/ST=EU/L=EU/O=KAFKA/OU=LOCAL"     # production: .../OU=TEST

force=0
users=()
hosts_env=${KAFKA_BROKER_HOSTS:-}
broker_hosts=(${hosts_env//,/ })        # "a b" or "a,b"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --force) force=1 ;;
        --broker-host) broker_hosts+=("$2"); shift ;;
        --broker-host=*) broker_hosts+=("${1#*=}") ;;
        -h|--help) sed -n '2,38p' "$0"; exit 0 ;;
        *) users+=("$1") ;;
    esac
    shift
done
[[ ${#users[@]} -eq 0 ]] && users=(upm)

[[ $force -eq 1 ]] && rm -rf "$ROOT"
umask 077
mkdir -p "$ROOT/ca" "$ROOT/broker"

rand() { openssl rand -hex 12; }

# sign <name> <dir> <cn> <san> [passphrase]
#   key + certificate signed by the local CA, RSA 2048 / SHA-256 as in production;
#   with a passphrase the key is PKCS#8 encrypted (AES-256).
sign() {
    local name=$1 dir=$2 cn=$3 san=$4 pass=${5:-}
    local key="$dir/$name.key" csr="$dir/$name.csr" crt="$dir/$name.certificate.pem"
    [[ -f "$crt" ]] && return 0
    if [[ -n "$pass" ]]; then
        openssl genpkey -quiet -algorithm RSA -pkeyopt rsa_keygen_bits:2048 \
            -aes-256-cbc -pass "pass:$pass" -out "$key"
        openssl req -new -key "$key" -passin "pass:$pass" -subj "$SUBJ/CN=$cn" -out "$csr"
    else
        openssl genpkey -quiet -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$key"
        openssl req -new -key "$key" -subj "$SUBJ/CN=$cn" -out "$csr"
    fi
    openssl x509 -req -in "$csr" -CA "$ROOT/ca/kafka-cedar-ca-1.crt" -CAkey "$ROOT/ca/ca.key" \
        -CAcreateserial -days $DAYS -sha256 -out "$crt" -extfile <(printf '%s\n' \
            "basicConstraints=CA:FALSE" \
            "keyUsage=digitalSignature,keyEncipherment" \
            "extendedKeyUsage=serverAuth,clientAuth" \
            "subjectAltName=$san") 2>/dev/null
    rm -f "$csr"
}

# ---- CA -------------------------------------------------------------------
if [[ ! -f "$ROOT/ca/kafka-cedar-ca-1.crt" ]]; then
    echo "creating the local CA"
    openssl genpkey -quiet -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$ROOT/ca/ca.key"
    openssl req -x509 -new -key "$ROOT/ca/ca.key" -sha256 -days $DAYS \
        -subj "$SUBJ/CN=ca1.local.cedarkafka.io" -out "$ROOT/ca/kafka-cedar-ca-1.crt" \
        -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign"
fi
cp "$ROOT/ca/kafka-cedar-ca-1.crt" "$ROOT/broker/"

# ---- broker: PEM keystore = key + certificate in one file -----------------
# names clients may use to reach it: Docker network, this machine, + --broker-host
broker_san="DNS:kafka,DNS:kafka-local,DNS:localhost,IP:127.0.0.1"
for h in "${broker_hosts[@]}"; do
    if [[ $h =~ ^[0-9]+(\.[0-9]+){3}$ ]]; then broker_san+=",IP:$h"; else broker_san+=",DNS:$h"; fi
done
if [[ -f "$ROOT/broker/broker.pem" && "$(cat "$ROOT/broker/broker.san" 2>/dev/null)" != "$broker_san" \
      && ${#broker_hosts[@]} -gt 0 ]]; then
    echo "broker host names changed: reissuing the broker certificate (CA and users kept)"
    rm -f "$ROOT/broker/broker.pem"
fi
if [[ ! -f "$ROOT/broker/broker.pem" ]]; then
    echo "creating the broker certificate for: ${broker_san//DNS:/}"
    sign broker "$ROOT/broker" kafka "$broker_san"
    cat "$ROOT/broker/broker.key" "$ROOT/broker/broker.certificate.pem" > "$ROOT/broker/broker.pem"
    rm "$ROOT/broker/broker.key" "$ROOT/broker/broker.certificate.pem"
    echo "$broker_san" > "$ROOT/broker/broker.san"
fi

# ---- admin: the broker's own SASL user, used by kafka-init ----------------
[[ -f "$ROOT/broker/admin.password" ]] || rand > "$ROOT/broker/admin.password"
admin_password=$(cat "$ROOT/broker/admin.password")
if [[ ! -f "$ROOT/broker/admin.pem" ]]; then
    sign admin "$ROOT/broker" admin "DNS:admin,DNS:localhost"
    cat "$ROOT/broker/admin.key" "$ROOT/broker/admin.certificate.pem" > "$ROOT/broker/admin.pem"
    rm "$ROOT/broker/admin.key" "$ROOT/broker/admin.certificate.pem"
fi
cat > "$ROOT/broker/admin-client.properties" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="$admin_password";
ssl.truststore.type=PEM
ssl.truststore.location=/etc/kafka/secrets/kafka-cedar-ca-1.crt
ssl.keystore.type=PEM
ssl.keystore.location=/etc/kafka/secrets/admin.pem
EOF

# ---- users: one folder each, laid out like the production archive ---------
for user in "${users[@]}"; do
    dir="$ROOT/$user"
    if [[ -f "$dir/cred.txt" ]]; then
        echo "user $user: already exists, kept"
        continue
    fi
    echo "creating user $user"
    mkdir -p "$dir"
    passphrase=$(rand)
    sign "$user" "$dir" "$user" "DNS:$user,DNS:localhost" "$passphrase"
    cp "$ROOT/ca/kafka-cedar-ca-1.crt" "$dir/"
    printf '%s\n%s\n%s\n' "$passphrase" "$user" "$(rand)" > "$dir/cred.txt"
done

# ---- broker SASL users: admin + every user folder ---------------------------
{
    echo "KafkaServer {"
    echo "    org.apache.kafka.common.security.plain.PlainLoginModule required"
    echo "    username=\"admin\""
    echo "    password=\"$admin_password\""
    echo "    user_admin=\"$admin_password\""
    for cred in "$ROOT"/*/cred.txt; do
        [[ -f "$cred" ]] || continue
        echo "    user_$(sed -n 2p "$cred")=\"$(sed -n 3p "$cred")\""
    done
} | sed '$ s/$/;/' > "$ROOT/broker/kafka_server_jaas.conf"     # ";" ends the last option
echo "};" >> "$ROOT/broker/kafka_server_jaas.conf"

# readable by the broker container (it does not run as this user)
chmod 755 "$ROOT" "$ROOT/broker"
chmod 644 "$ROOT"/broker/*

echo
echo "done: $ROOT"
for user in "${users[@]}"; do
    echo "  $user -> $ROOT/$user/   (fill .env from $user/cred.txt, as in production)"
done
