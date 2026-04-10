#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_PYTHON_BIN="${BASE_PYTHON_BIN:-python3}"
VENV_DIR="${VENV_DIR:-$APP_DIR/venv}"
VENV_PYTHON="$VENV_DIR/bin/python"
VENV_PIP="$VENV_DIR/bin/pip"
PYTHON_BIN="${PYTHON_BIN:-$VENV_PYTHON}"
UNDERLYINGS="${UNDERLYINGS:-BTC}"
OUTPUT_DIR="${OUTPUT_DIR:-$APP_DIR/data}"
LOG_DIR="${LOG_DIR:-$APP_DIR/logs}"
RUN_DIR="${RUN_DIR:-$APP_DIR/run}"
LOG_FILE="${LOG_FILE:-$LOG_DIR/collector.log}"
PID_FILE="${PID_FILE:-$RUN_DIR/collector.pid}"
REQUIREMENTS_FILE="${REQUIREMENTS_FILE:-$APP_DIR/requirements.txt}"
STARTUP_TIMEOUT="${STARTUP_TIMEOUT:-5}"
STOP_TIMEOUT="${STOP_TIMEOUT:-20}"

mkdir -p "$LOG_DIR" "$RUN_DIR"

CMD=(
  "$PYTHON_BIN"
  "$APP_DIR/collector_main.py"
  run
  --underlyings "$UNDERLYINGS"
  --output-dir "$OUTPUT_DIR"
)

command_string() {
  printf "%q " "${CMD[@]}"
}

ensure_virtualenv() {
  if [[ -x "$VENV_PYTHON" && -x "$VENV_PIP" ]]; then
    return 0
  fi

  echo "creating virtual environment: $VENV_DIR"
  "$BASE_PYTHON_BIN" -m venv "$VENV_DIR"
}

ensure_requirements() {
  if "$PYTHON_BIN" -c "import websockets" >/dev/null 2>&1; then
    return 0
  fi

  if [[ ! -f "$REQUIREMENTS_FILE" ]]; then
    echo "requirements file not found: $REQUIREMENTS_FILE" >&2
    exit 1
  fi

  echo "installing dependencies from $REQUIREMENTS_FILE"
  "$VENV_PIP" install -r "$REQUIREMENTS_FILE"
}

is_pid_running() {
  local pid="$1"
  [[ -n "$pid" ]] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  [[ -r "/proc/$pid/cmdline" ]] || return 1
  tr '\0' ' ' <"/proc/$pid/cmdline" | grep -Fq "collector_main.py"
}

current_pid() {
  if [[ -f "$PID_FILE" ]]; then
    tr -d '[:space:]' <"$PID_FILE"
  fi
}

ensure_not_running() {
  local pid
  pid="$(current_pid || true)"
  if [[ -n "${pid:-}" ]] && is_pid_running "$pid"; then
    echo "collector is already running, pid=$pid"
    exit 0
  fi

  if [[ -n "${pid:-}" ]]; then
    rm -f "$PID_FILE"
  fi
}

start_collector() {
  ensure_not_running
  ensure_virtualenv
  ensure_requirements

  echo "starting collector..."
  echo "[$(date '+%F %T')] starting: $(command_string)" >>"$LOG_FILE"
  nohup "${CMD[@]}" >>"$LOG_FILE" 2>&1 &
  local pid=$!
  echo "$pid" >"$PID_FILE"

  sleep "$STARTUP_TIMEOUT"
  if is_pid_running "$pid"; then
    echo "collector started, pid=$pid"
    echo "log file: $LOG_FILE"
    exit 0
  fi

  echo "collector failed to stay running, check log: $LOG_FILE" >&2
  rm -f "$PID_FILE"
  exit 1
}

stop_collector() {
  local pid
  pid="$(current_pid || true)"

  if [[ -z "${pid:-}" ]]; then
    echo "collector is not running"
    exit 0
  fi

  if ! is_pid_running "$pid"; then
    echo "stale pid file found, removing $PID_FILE"
    rm -f "$PID_FILE"
    exit 0
  fi

  echo "stopping collector, pid=$pid"
  kill "$pid"

  local waited=0
  while is_pid_running "$pid"; do
    if (( waited >= STOP_TIMEOUT )); then
      echo "collector did not stop within ${STOP_TIMEOUT}s, sending SIGKILL"
      kill -9 "$pid" 2>/dev/null || true
      break
    fi
    sleep 1
    waited=$((waited + 1))
  done

  rm -f "$PID_FILE"
  echo "collector stopped"
}

status_collector() {
  local pid
  pid="$(current_pid || true)"

  if [[ -n "${pid:-}" ]] && is_pid_running "$pid"; then
    echo "collector is running, pid=$pid"
    echo "log file: $LOG_FILE"
    exit 0
  fi

  echo "collector is not running"
  exit 1
}

restart_collector() {
  stop_collector
  start_collector
}

usage() {
  cat <<EOF
Usage: $(basename "$0") {start|stop|restart|status}

Environment overrides:
  BASE_PYTHON_BIN Base Python used to create venv, default: python3
  VENV_DIR        Virtualenv directory, default: $APP_DIR/venv
  PYTHON_BIN      Python executable used to run collector, default: $VENV_PYTHON
  UNDERLYINGS     Underlyings passed to the collector, default: BTC
  OUTPUT_DIR      Collector CSV output directory, default: $APP_DIR/data
  LOG_DIR         Directory for logs, default: $APP_DIR/logs
  LOG_FILE        Collector stdout/stderr log file, default: $LOG_FILE
  RUN_DIR         Directory for pid files, default: $APP_DIR/run
  REQUIREMENTS_FILE Requirements file, default: $REQUIREMENTS_FILE
  STARTUP_TIMEOUT Seconds to wait after start, default: 5
  STOP_TIMEOUT    Seconds to wait before SIGKILL, default: 20
EOF
}

ACTION="${1:-}"

case "$ACTION" in
  start)
    start_collector
    ;;
  stop)
    stop_collector
    ;;
  restart)
    restart_collector
    ;;
  status)
    status_collector
    ;;
  *)
    usage
    exit 1
    ;;
esac
