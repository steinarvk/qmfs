stop_qmfs() {
  if [[ -d "${Q}/service" ]]; then
    kill $(cat "${Q}/service/pid")
    fusermount -u "${Q}" || true
  fi
}

start_qmfs() {
  fusermount -u "${Q}" || true
  ./qmfs serve --mountpoint "${Q}" --localdb "${QMFS_TEST_TEMP}/database.sqlite3" > /dev/null 2> /dev/null &
  for n in $(seq 1000); do
    if [[ ! -d "${Q}/service" ]]; then
      sleep 0.1
    fi
  done
}

restart_qmfs() {
  stop_qmfs
  start_qmfs
}

setup() {
  export QMFS_TEST_TEMP="${BATS_TMPDIR}/qmfs-test-temp"
  mkdir -p "${QMFS_TEST_TEMP}"

  export Q="${QMFS_TEST_TEMP}/mountpoint"
  mkdir "${Q}"

  start_qmfs
}

teardown() {
  stop_qmfs

  if [[ -n "${QMFS_TEST_TEMP}" ]]; then
    rm -rf "${QMFS_TEST_TEMP}"
  fi
}
