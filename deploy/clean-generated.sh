#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd -P)"

CHECK_ONLY=0
if [[ "${1:-}" == --check ]]; then
  CHECK_ONLY=1
  shift
fi
if [[ "$#" -ne 1 ]]; then
  echo "Usage: $0 [--check] <repository-relative-generated-directory>" >&2
  exit 2
fi

REQUESTED="$1"
if [[ -z "${REQUESTED}" || "${REQUESTED}" == /* || ! "${REQUESTED}" =~ ^[A-Za-z0-9._/-]+$ ]]; then
  echo "Unsafe generated-directory path: ${REQUESTED:-<empty>}" >&2
  exit 2
fi

old_ifs="${IFS}"
IFS=/
read -r -a components <<<"${REQUESTED}"
IFS="${old_ifs}"
if [[ "${#components[@]}" -eq 0 ]]; then
  echo "Generated-directory path is empty." >&2
  exit 2
fi

current="${ROOT_DIR}"
for component in "${components[@]}"; do
  if [[ -z "${component}" || "${component}" == . || "${component}" == .. || "${component}" == .git ]]; then
    echo "Generated-directory path contains an unsafe component: ${REQUESTED}" >&2
    exit 2
  fi
  current="${current}/${component}"
  if [[ -L "${current}" ]]; then
    echo "Generated-directory path must not contain symlink components: ${current}" >&2
    exit 2
  fi
done

TARGET="$(readlink -m -- "${ROOT_DIR}/${REQUESTED}")"
case "${TARGET}" in
  "${ROOT_DIR}/"*) ;;
  *)
    echo "Generated-directory path escaped the repository: ${TARGET}" >&2
    exit 2
    ;;
esac
RELATIVE="${TARGET#"${ROOT_DIR}/"}"

if [[ -n "$(git -C "${ROOT_DIR}" ls-files -- "${RELATIVE}")" ]]; then
  echo "Refusing to clean a directory that overlaps tracked source: ${RELATIVE}" >&2
  exit 2
fi
if ! git -C "${ROOT_DIR}" check-ignore -q -- "${RELATIVE}" &&
   ! git -C "${ROOT_DIR}" check-ignore -q -- "${RELATIVE}/.traffic-go-clean-probe"; then
  echo "Refusing to clean a directory that is not ignored by Git: ${RELATIVE}" >&2
  exit 2
fi
if [[ -e "${TARGET}" && ! -d "${TARGET}" ]]; then
  echo "Generated clean target must be a directory: ${TARGET}" >&2
  exit 2
fi
if command -v mountpoint >/dev/null 2>&1 && mountpoint -q -- "${TARGET}"; then
  echo "Refusing to clean a filesystem mount point: ${TARGET}" >&2
  exit 2
fi

if [[ "${CHECK_ONLY}" -eq 1 ]]; then
  printf 'Safe generated directory: %s\n' "${TARGET}"
  exit 0
fi

rm -rf -- "${TARGET}"
printf 'Removed generated directory: %s\n' "${TARGET}"
