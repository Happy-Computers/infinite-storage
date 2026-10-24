#!/usr/bin/env bash
set -euo pipefail

readonly minio_version="RELEASE.2025-09-07T16-13-09Z"
readonly minio_sha256="7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f"
readonly minio_url="https://github.com/minio/minio/releases/download/${minio_version}/"\
"minio.linux-amd64.${minio_version}"
readonly minio_cache_dir="/tmp/opencode/infinitefs-minio"
readonly endpoint="127.0.0.1:19000"

minio_bin="${MINIO_BIN:-${minio_cache_dir}/minio}"
run_dir="$(mktemp -d /tmp/opencode/infinitefs-minio-run.XXXXXX)"
log_file="${run_dir}/minio.log"
minio_pid=""
source_file=""
output_file=""

usage() {
    echo "Usage: $0 [SOURCE_FILE OUTPUT_FILE]" >&2
}

validate_arguments() {
    if [[ "$#" -eq 0 ]]; then
        return
    fi
    if [[ "$#" -ne 2 ]]; then
        usage
        exit 1
    fi

    source_file="$1"
    output_file="$2"
    if [[ ! -f "${source_file}" ]] || [[ -L "${source_file}" ]]; then
        echo "SOURCE_FILE must be a regular file: ${source_file}" >&2
        exit 1
    fi
    if [[ -e "${output_file}" ]] || [[ -L "${output_file}" ]]; then
        echo "OUTPUT_FILE already exists: ${output_file}" >&2
        exit 1
    fi
    if [[ ! -d "$(dirname "${output_file}")" ]]; then
        echo "OUTPUT_FILE parent directory does not exist: ${output_file}" >&2
        exit 1
    fi
}

install_minio() {
    if [[ -x "${minio_bin}" ]] && [[ "${MINIO_BIN:-}" == "" ]]; then
        if [[ "$(sha256sum "${minio_bin}" | awk '{print $1}')" == "${minio_sha256}" ]]; then
            return
        fi
    fi

    if [[ "${MINIO_BIN:-}" != "" ]]; then
        if [[ ! -x "${minio_bin}" ]]; then
            echo "MINIO_BIN is not executable: ${minio_bin}" >&2
            exit 1
        fi
        return
    fi

    mkdir -p "${minio_cache_dir}"
    curl --fail --location --silent --show-error --output "${minio_bin}.download" "${minio_url}"
    echo "${minio_sha256}  ${minio_bin}.download" | sha256sum --check --status
    chmod 755 "${minio_bin}.download"
    mv "${minio_bin}.download" "${minio_bin}"
}

cleanup() {
    local status=$?
    trap - EXIT INT TERM

    if [[ -n "${minio_pid}" ]] && kill -0 "${minio_pid}" 2>/dev/null; then
        kill "${minio_pid}"
    fi
    if [[ -n "${minio_pid}" ]]; then
        wait "${minio_pid}" 2>/dev/null || true
    fi

    if [[ "${status}" -eq 0 ]]; then
        rm -rf "${run_dir}"
    else
        echo "MinIO logs remain at ${log_file}" >&2
    fi
    exit "${status}"
}

wait_for_minio() {
    for _ in {1..100}; do
        if curl --fail --silent "http://${endpoint}/minio/health/live" >/dev/null; then
            return
        fi
        if ! kill -0 "${minio_pid}" 2>/dev/null; then
            cat "${log_file}" >&2
            return 1
        fi
        sleep 0.1
    done

    cat "${log_file}" >&2
    return 1
}

validate_arguments "$@"
trap cleanup EXIT INT TERM
install_minio

MINIO_ROOT_USER="infinitefs-test" \
MINIO_ROOT_PASSWORD="infinitefs-test-secret" \
MINIO_BROWSER="off" \
"${minio_bin}" server "${run_dir}/data" --address "${endpoint}" >"${log_file}" 2>&1 &
minio_pid=$!
wait_for_minio

if [[ -n "${source_file}" ]]; then
    INFINITEFS_MINIO_SOURCE="${source_file}" \
    INFINITEFS_MINIO_OUTPUT="${output_file}" \
    INFINITEFS_MINIO_ENDPOINT="http://${endpoint}" \
    INFINITEFS_MINIO_ACCESS_KEY="infinitefs-test" \
    INFINITEFS_MINIO_SECRET_KEY="infinitefs-test-secret" \
    cargo test --locked --features minio-integration --test minio_stream -- --nocapture
else
    INFINITEFS_MINIO_ENDPOINT="http://${endpoint}" \
    INFINITEFS_MINIO_ACCESS_KEY="infinitefs-test" \
    INFINITEFS_MINIO_SECRET_KEY="infinitefs-test-secret" \
    cargo test --locked --features minio-integration --test minio_stream -- --nocapture
fi
