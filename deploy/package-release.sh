#!/usr/bin/env bash
set -euo pipefail
umask 022
export LC_ALL=C

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd -P)"

APP_NAME="${APP_NAME:-traffic-go}"
GO_BIN="${GO:-go}"
NPM_BIN="${NPM:-npm}"
NODE_BIN="${NODE:-node}"
WEB_DIR="${WEB_DIR:-web}"
RELEASE_ROOT="${RELEASE_ROOT:-release}"
ALLOW_DIRTY="${ALLOW_DIRTY:-0}"

if [[ ! "${APP_NAME}" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ || "${APP_NAME}" == *..* ]]; then
  echo "APP_NAME must be a single safe basename: ${APP_NAME}" >&2
  exit 2
fi
if [[ "${APP_NAME}" != traffic-go ]]; then
  echo "The packaged service and installer require APP_NAME=traffic-go; found ${APP_NAME}." >&2
  exit 2
fi
if [[ "${WEB_DIR}" == /* || "${WEB_DIR}" == *..* || "${WEB_DIR}" == . || "/${WEB_DIR}/" == */.git/* || ! "${WEB_DIR}" =~ ^[A-Za-z0-9._/-]+$ ]]; then
  echo "Unsafe WEB_DIR: ${WEB_DIR}" >&2
  exit 2
fi
if [[ "${RELEASE_ROOT}" == /* || "${RELEASE_ROOT}" == *..* || "${RELEASE_ROOT}" == . || "/${RELEASE_ROOT}/" == */.git/* || ! "${RELEASE_ROOT}" =~ ^[A-Za-z0-9._/-]+$ ]]; then
  echo "Unsafe RELEASE_ROOT: ${RELEASE_ROOT}" >&2
  exit 2
fi

resolve_tool() {
  local requested="$1"
  local label="$2"
  local candidate candidate_dir candidate_name

  if [[ "${requested}" == */* ]]; then
    if [[ "${requested}" == /* ]]; then
      candidate="${requested}"
    else
      candidate="${ROOT_DIR}/${requested}"
    fi
    candidate_dir="$(dirname -- "${candidate}")"
    candidate_name="$(basename -- "${candidate}")"
    if [[ ! -d "${candidate_dir}" ]]; then
      echo "${label} executable directory does not exist: ${candidate_dir}" >&2
      return 1
    fi
    candidate="$(cd "${candidate_dir}" && pwd -P)/${candidate_name}"
  else
    candidate="$(command -v -- "${requested}" 2>/dev/null || true)"
  fi

  if [[ -z "${candidate}" || ! -f "${candidate}" || ! -x "${candidate}" ]]; then
    echo "${label} executable is unavailable: ${requested}" >&2
    return 1
  fi
  printf '%s\n' "${candidate}"
}

run_npm() {
  if [[ "${NPM_BIN,,}" == *.cmd ]]; then
    cmd.exe //c "${NPM_BIN} $*"
    return
  fi
  "${NPM_BIN}" "$@"
}

run_go() {
  env GOENV=off GOWORK=off GOFLAGS= "${GO_BIN}" "$@"
}

GO_BIN="$(resolve_tool "${GO_BIN}" Go)"
NPM_BIN="$(resolve_tool "${NPM_BIN}" npm)"
NODE_BIN="$(resolve_tool "${NODE_BIN}" Node.js)"

NODE_VERSION="$("${NODE_BIN}" --version | tr -d '\r\n')"
NPM_VERSION="$(run_npm --version | tr -d '\r\n')"
if [[ ! "${NODE_VERSION}" =~ ^v?([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
  echo "Release builds require a stable Node.js toolchain; found ${NODE_VERSION}." >&2
  exit 1
fi
NODE_VERSION_NUMBER="${NODE_VERSION#v}"
if [[ ! "${NPM_VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Release builds require a stable npm toolchain; found ${NPM_VERSION}." >&2
  exit 1
fi

GO_VERSION="$(run_go env GOVERSION)"
GO_HOST_OS="$(run_go env GOHOSTOS)"
GO_HOST_ARCH="$(run_go env GOHOSTARCH)"
if [[ ! "${GO_VERSION}" =~ ^go([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
  echo "Release builds require a stable Go toolchain; found ${GO_VERSION}." >&2
  exit 1
fi
GO_MAJOR="${BASH_REMATCH[1]}"
GO_MINOR="${BASH_REMATCH[2]}"
GO_PATCH="${BASH_REMATCH[3]}"
if (( GO_MAJOR < 1 || (GO_MAJOR == 1 && GO_MINOR < 26) || (GO_MAJOR == 1 && GO_MINOR == 26 && GO_PATCH < 5) )); then
  echo "Release builds require Go 1.26.5 or newer; found ${GO_VERSION}." >&2
  exit 1
fi
COMMIT="$(git -C "${ROOT_DIR}" rev-parse HEAD)"
SHORT_COMMIT="$(git -C "${ROOT_DIR}" rev-parse --short=12 HEAD)"
EXACT_TAG="$(git -C "${ROOT_DIR}" describe --tags --exact-match HEAD 2>/dev/null || true)"
REQUESTED_VERSION="${VERSION:-${EXACT_TAG}}"
if [[ -z "${REQUESTED_VERSION}" ]]; then
  echo "Release builds require HEAD to have an exact tag, or VERSION to be set." >&2
  exit 1
fi
VERSION_NUMBER="${REQUESTED_VERSION#v}"
SEMVER_PATTERN='^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z]+([.-][0-9A-Za-z]+)*)?(\+[0-9A-Za-z]+([.-][0-9A-Za-z]+)*)?$'
if [[ ! "${VERSION_NUMBER}" =~ ${SEMVER_PATTERN} ]]; then
  echo "Release version must be a safe SemVer value, found: ${VERSION_NUMBER}" >&2
  exit 1
fi
EXPECTED_TAG="v${VERSION_NUMBER}"
TAG_COMMIT="$(git -C "${ROOT_DIR}" rev-list -n 1 "${EXPECTED_TAG}" 2>/dev/null || true)"
if [[ "${TAG_COMMIT}" != "${COMMIT}" ]]; then
  echo "Version ${VERSION_NUMBER} does not identify a tag at HEAD (${SHORT_COMMIT})." >&2
  exit 1
fi

DIRTY_STATUS="$(git -C "${ROOT_DIR}" status --porcelain --untracked-files=normal)"
if [[ -n "${DIRTY_STATUS}" && "${ALLOW_DIRTY}" != 1 ]]; then
  echo "Refusing to package a dirty worktree. Commit or stash all changes first." >&2
  printf '%s\n' "${DIRTY_STATUS}" >&2
  exit 1
fi
if [[ -n "${DIRTY_STATUS}" ]]; then
  VERSION_NUMBER="${VERSION_NUMBER%%+*}-dirty.${SHORT_COMMIT}"
fi

PINNED_NODE_VERSION="$(git -C "${ROOT_DIR}" show "${COMMIT}:.node-version" 2>/dev/null | tr -d '\r\n' || true)"
PINNED_NPM_VERSION="$(git -C "${ROOT_DIR}" show "${COMMIT}:${WEB_DIR}/package.json" 2>/dev/null | sed -nE 's/.*"packageManager"[[:space:]]*:[[:space:]]*"npm@([^\"]+)".*/\1/p' | head -n 1 || true)"
if [[ ! "${PINNED_NODE_VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ || "${NODE_VERSION_NUMBER}" != "${PINNED_NODE_VERSION}" ]]; then
  echo "Release builds require Node.js ${PINNED_NODE_VERSION:-<missing .node-version>}; found ${NODE_VERSION}." >&2
  exit 1
fi
if [[ ! "${PINNED_NPM_VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ || "${NPM_VERSION}" != "${PINNED_NPM_VERSION}" ]]; then
  echo "Release builds require npm ${PINNED_NPM_VERSION:-<missing packageManager>}; found ${NPM_VERSION}." >&2
  exit 1
fi

verify_source_state() {
  local current_head current_status current_tag_commit
  current_head="$(git -C "${ROOT_DIR}" rev-parse HEAD)"
  current_status="$(git -C "${ROOT_DIR}" status --porcelain --untracked-files=normal)"
  current_tag_commit="$(git -C "${ROOT_DIR}" rev-list -n 1 "${EXPECTED_TAG}" 2>/dev/null || true)"
  if [[ "${current_head}" != "${COMMIT}" || "${current_status}" != "${DIRTY_STATUS}" || "${current_tag_commit}" != "${COMMIT}" ]]; then
    echo "Source HEAD or worktree state changed during release packaging; refusing mixed provenance." >&2
    return 1
  fi
}

copy_release_asset() {
  local source_path="$1"
  local destination="$2"
  if [[ ! -f "${BUILD_ROOT}/${source_path}" || -L "${BUILD_ROOT}/${source_path}" ]]; then
    echo "Release asset must be a regular file from the clean source snapshot: ${source_path}" >&2
    return 1
  fi
  cp -- "${BUILD_ROOT}/${source_path}" "${destination}"
}

SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-$(git -C "${ROOT_DIR}" show -s --format=%ct "${COMMIT}")}"
if [[ ! "${SOURCE_DATE_EPOCH}" =~ ^[0-9]+$ ]]; then
  echo "SOURCE_DATE_EPOCH must be a non-negative integer." >&2
  exit 1
fi
export SOURCE_DATE_EPOCH
BUILD_DATE="$(date -u -d "@${SOURCE_DATE_EPOCH}" +%Y-%m-%dT%H:%M:%SZ)"
ARCHIVE_NAME="${APP_NAME}-v${VERSION_NUMBER}-linux-amd64.tar.gz"
RELEASE_BASE="$(readlink -m -- "${ROOT_DIR}/${RELEASE_ROOT}")"
case "${RELEASE_BASE}" in
  "${ROOT_DIR}/"*) ;;
  *)
    echo "Release root must be a strict descendant of the repository: ${RELEASE_BASE}" >&2
    exit 2
    ;;
esac
RELEASE_RELATIVE="${RELEASE_BASE#"${ROOT_DIR}/"}"
if [[ -n "$(git -C "${ROOT_DIR}" ls-files -- "${RELEASE_RELATIVE}")" ]]; then
  echo "Release root must not overlap tracked source files: ${RELEASE_ROOT}" >&2
  exit 2
fi
if ! git -C "${ROOT_DIR}" check-ignore -q -- "${RELEASE_RELATIVE}/.traffic-go-release-probe"; then
  echo "Release root must be ignored by Git so packaging cannot dirty source state: ${RELEASE_ROOT}" >&2
  exit 2
fi
FINAL_VERSION_ROOT="${RELEASE_BASE}/v${VERSION_NUMBER}"
FINAL_RELEASE_DIR="${FINAL_VERSION_ROOT}/linux-amd64"
FINAL_ARCHIVE_PATH="${FINAL_VERSION_ROOT}/${ARCHIVE_NAME}"
FINAL_ARCHIVE_CHECKSUM="${FINAL_ARCHIVE_PATH}.sha256"
EMBED_DIR=""
EXPECTED_BUILD_ID="${APP_NAME}/${VERSION_NUMBER}/${COMMIT}"
LDFLAGS="-s -w -buildid=${EXPECTED_BUILD_ID} -X traffic-go/internal/buildinfo.Version=${VERSION_NUMBER} -X traffic-go/internal/buildinfo.Commit=${COMMIT} -X traffic-go/internal/buildinfo.BuildDate=${BUILD_DATE}"
LOCK_GIT_PATH="$(git -C "${ROOT_DIR}" rev-parse --git-path traffic-go-build.lock)"
if [[ "${LOCK_GIT_PATH}" != /* ]]; then
  LOCK_GIT_PATH="${ROOT_DIR}/${LOCK_GIT_PATH}"
fi
LOCK_DIR_EXPECTED="$(readlink -m -- "${LOCK_GIT_PATH}")"
LOCK_DIR=""
LOCK_HELD=0
STAGING_DIR=""
STAGING_OWNED=0
SOURCE_TEMP_ROOT=""
SOURCE_HOOKS_DIR=""
SOURCE_TREE=""
BUILD_ROOT=""
BUILD_WEB_ROOT=""

verify_build_source_state() {
  local build_head build_status
  build_head="$(git -C "${BUILD_ROOT}" rev-parse HEAD)"
  build_status="$(git -C "${BUILD_ROOT}" status --porcelain --untracked-files=normal)"
  if [[ "${build_head}" != "${COMMIT}" || -n "${build_status}" ]]; then
    echo "Clean source snapshot changed during release packaging; refusing mixed provenance." >&2
    [[ -z "${build_status}" ]] || printf '%s\n' "${build_status}" >&2
    return 1
  fi
}

verify_pristine_build_source() {
  local build_status
  verify_build_source_state
  build_status="$(git -C "${BUILD_ROOT}" status --porcelain --untracked-files=all --ignored)"
  if [[ -n "${build_status}" ]]; then
    echo "Temporary source snapshot unexpectedly contains untracked or ignored inputs." >&2
    printf '%s\n' "${build_status}" >&2
    return 1
  fi
}

verify_release_base() {
  local actual_release_base
  if [[ ! -d "${RELEASE_BASE}" || -L "${RELEASE_BASE}" ]]; then
    echo "Release root must remain a real directory: ${RELEASE_BASE}" >&2
    return 1
  fi
  actual_release_base="$(cd "${RELEASE_BASE}" && pwd -P)"
  if [[ "${actual_release_base}" != "${RELEASE_BASE}" ]]; then
    echo "Release root changed or resolves through a symlink: ${RELEASE_BASE}" >&2
    return 1
  fi
}

cleanup_staging() {
  local source_tree_removed=0
  set +e
  if [[ -n "${SOURCE_TEMP_ROOT:-}" ]]; then
    if [[ "${SOURCE_TEMP_ROOT}" == /tmp/traffic-go-release-source.* && "${SOURCE_TREE:-}" == "${SOURCE_TEMP_ROOT}/worktree" && "${SOURCE_HOOKS_DIR:-}" == "${SOURCE_TEMP_ROOT}/empty-hooks" ]]; then
      if git -C "${ROOT_DIR}" worktree remove --force -- "${SOURCE_TREE}" >/dev/null 2>&1; then
        source_tree_removed=1
      elif [[ ! -e "${SOURCE_TREE}" && ! -L "${SOURCE_TREE}" ]]; then
        source_tree_removed=1
      elif [[ -d "${SOURCE_TREE}" && ! -L "${SOURCE_TREE}" ]] && rmdir -- "${SOURCE_TREE}" >/dev/null 2>&1; then
        source_tree_removed=1
      fi
      if [[ "${source_tree_removed}" -eq 0 ]]; then
        echo "Unable to remove temporary clean source worktree; preserving it for inspection: ${SOURCE_TREE}" >&2
      else
        rmdir -- "${SOURCE_HOOKS_DIR}" 2>/dev/null || true
        if ! rmdir -- "${SOURCE_TEMP_ROOT}" 2>/dev/null; then
          echo "Temporary source root contains unexpected files; preserving it for inspection: ${SOURCE_TEMP_ROOT}" >&2
        fi
      fi
    else
      echo "Refusing to clean unexpected source worktree: ${SOURCE_TREE:-<unset>}" >&2
    fi
  fi
  if [[ "${STAGING_OWNED:-0}" -eq 1 && -n "${STAGING_DIR:-}" && -d "${STAGING_DIR}" ]]; then
    case "${STAGING_DIR}" in
      "${RELEASE_BASE}/.${APP_NAME}-v${VERSION_NUMBER}."??????)
        if ! verify_release_base; then
          echo "Refusing to clean staging through a changed release root: ${STAGING_DIR}" >&2
        elif ! rm -rf -- "${STAGING_DIR}"; then
          echo "Unable to remove temporary release staging directory: ${STAGING_DIR}" >&2
        fi
        ;;
      *) echo "Refusing to clean unexpected staging path: ${STAGING_DIR}" >&2 ;;
    esac
  fi
  if [[ "${LOCK_HELD:-0}" -eq 1 && -n "${LOCK_DIR:-}" ]]; then
    case "${LOCK_DIR}" in
      "${LOCK_DIR_EXPECTED}") rmdir -- "${LOCK_DIR}" 2>/dev/null || true ;;
      *) echo "Refusing to clean unexpected release lock: ${LOCK_DIR}" >&2 ;;
    esac
  fi
}
trap cleanup_staging EXIT

reject_symlink_components() {
  local current="${ROOT_DIR}"
  local component
  local old_ifs="${IFS}"
  local -a components=()
  IFS=/
  read -r -a components <<<"${RELEASE_ROOT}"
  IFS="${old_ifs}"
  for component in "${components[@]}"; do
    [[ -n "${component}" && "${component}" != . ]] || continue
    current="${current}/${component}"
    if [[ -L "${current}" ]]; then
      echo "Release path must not contain symlink components: ${current}" >&2
      exit 1
    fi
  done
}

reject_symlink_components
mkdir -p "${RELEASE_BASE}"
verify_release_base
LOCK_DIR="${LOCK_DIR_EXPECTED}"
if ! mkdir "${LOCK_DIR}" 2>/dev/null; then
  echo "Another build or a stale release lock exists: ${LOCK_DIR}" >&2
  exit 1
fi
LOCK_HELD=1
verify_source_state
if [[ -e "${FINAL_VERSION_ROOT}" || -L "${FINAL_VERSION_ROOT}" ]]; then
  echo "Versioned release output already exists and will not be replaced: ${FINAL_VERSION_ROOT}" >&2
  exit 1
fi
SOURCE_TEMP_ROOT="$(mktemp -d /tmp/traffic-go-release-source.XXXXXX)"
SOURCE_HOOKS_DIR="${SOURCE_TEMP_ROOT}/empty-hooks"
SOURCE_TREE="${SOURCE_TEMP_ROOT}/worktree"
mkdir "${SOURCE_HOOKS_DIR}"
git -c core.hooksPath="${SOURCE_HOOKS_DIR}" -C "${ROOT_DIR}" worktree add --detach -- "${SOURCE_TREE}" "${COMMIT}" >/dev/null
BUILD_ROOT="${SOURCE_TREE}"
verify_pristine_build_source
BUILD_WEB_ROOT="$(readlink -m -- "${BUILD_ROOT}/${WEB_DIR}")"
if [[ ! -d "${BUILD_WEB_ROOT}" || -L "${BUILD_WEB_ROOT}" || "$(cd "${BUILD_WEB_ROOT}" && pwd -P)" != "${BUILD_WEB_ROOT}" ]]; then
  echo "Frontend source directory must be a real path inside the clean source snapshot: ${BUILD_WEB_ROOT}" >&2
  exit 1
fi
case "${BUILD_WEB_ROOT}" in
  "${BUILD_ROOT}/"*) ;;
  *)
    echo "Frontend source directory escaped the clean source snapshot: ${BUILD_WEB_ROOT}" >&2
    exit 1
    ;;
esac
EMBED_DIR="${BUILD_ROOT}/internal/embed/dist"
mkdir -p "${EMBED_DIR}"
if [[ -L "${EMBED_DIR}" || "$(cd "${EMBED_DIR}" && pwd -P)" != "${EMBED_DIR}" ]]; then
  echo "Embedded asset directory must be the real build-source path, not a symlink: ${EMBED_DIR}" >&2
  exit 1
fi
verify_release_base
STAGING_DIR="$(mktemp -d "${RELEASE_BASE}/.${APP_NAME}-v${VERSION_NUMBER}.XXXXXX")"
STAGING_OWNED=1
RELEASE_DIR="${STAGING_DIR}/linux-amd64"
ARCHIVE_PATH="${STAGING_DIR}/${ARCHIVE_NAME}"
mkdir -p "${RELEASE_DIR}"

run_npm --prefix "${BUILD_WEB_ROOT}" ci
run_npm --prefix "${BUILD_WEB_ROOT}" run test
run_npm --prefix "${BUILD_WEB_ROOT}" audit --audit-level=low
run_npm --prefix "${BUILD_WEB_ROOT}" run build
if [[ ! -s "${BUILD_WEB_ROOT}/dist/index.html" ]]; then
  echo "Frontend build did not produce dist/index.html." >&2
  exit 1
fi
find "${EMBED_DIR}" -mindepth 1 -maxdepth 1 ! -name placeholder.txt -exec rm -rf -- {} +
cp -R "${BUILD_WEB_ROOT}/dist/." "${EMBED_DIR}/"
if [[ ! -f "${EMBED_DIR}/placeholder.txt" || -L "${EMBED_DIR}/placeholder.txt" ]]; then
  echo "Tracked embedded placeholder is missing or unsafe." >&2
  exit 1
fi

(
  cd "${BUILD_ROOT}"
  run_go test ./...
  run_go vet ./...
  GOOS=linux GOARCH=amd64 CGO_ENABLED=0 run_go build \
    -buildvcs=false \
    -trimpath \
    -ldflags "${LDFLAGS}" \
    -o "${RELEASE_DIR}/${APP_NAME}" \
    "./cmd/${APP_NAME}"
)
verify_build_source_state
chmod 0755 "${RELEASE_DIR}/${APP_NAME}"
if [[ "${GO_HOST_OS}/${GO_HOST_ARCH}" == linux/amd64 ]]; then
  VERSION_OUTPUT="$("${RELEASE_DIR}/${APP_NAME}" -version)"
else
  HOST_VERSION_PROBE="${STAGING_DIR}/.${APP_NAME}-version-probe"
  if [[ "${GO_HOST_OS}" == windows ]]; then
    HOST_VERSION_PROBE="${HOST_VERSION_PROBE}.exe"
  fi
  (
    cd "${BUILD_ROOT}"
    run_go build \
      -buildvcs=false \
      -trimpath \
      -ldflags "${LDFLAGS}" \
      -o "${HOST_VERSION_PROBE}" \
      "./cmd/${APP_NAME}"
  )
  VERSION_OUTPUT="$("${HOST_VERSION_PROBE}" -version)"
  rm -f -- "${HOST_VERSION_PROBE}"
  verify_build_source_state
fi
for expected_field in \
  "version=${VERSION_NUMBER}" \
  "commit=${COMMIT}" \
  "built=${BUILD_DATE}"; do
  if [[ " ${VERSION_OUTPUT} " != *" ${expected_field} "* ]]; then
    echo "Built binary provenance mismatch; expected ${expected_field}, got: ${VERSION_OUTPUT}" >&2
    exit 1
  fi
done

ACTUAL_BUILD_ID="$(run_go tool buildid "${RELEASE_DIR}/${APP_NAME}")"
if [[ "${ACTUAL_BUILD_ID}" != "${EXPECTED_BUILD_ID}" ]]; then
  echo "Built binary build ID mismatch; expected ${EXPECTED_BUILD_ID}, got ${ACTUAL_BUILD_ID}." >&2
  exit 1
fi

{
  printf '%s: %s\n' "${APP_NAME}" "${GO_VERSION}"
  printf 'node: %s\n' "${NODE_VERSION}"
  printf 'npm: %s\n' "${NPM_VERSION}"
  printf 'source.commit: %s\n' "${COMMIT}"
  printf 'source.tag: %s\n' "${EXPECTED_TAG}"
  printf 'build.id: %s\n' "${ACTUAL_BUILD_ID}"
  run_go version -m "${RELEASE_DIR}/${APP_NAME}" | sed '1d'
} >"${RELEASE_DIR}/BUILD-INFO.txt"
if ! grep -Fxq "source.commit: ${COMMIT}" "${RELEASE_DIR}/BUILD-INFO.txt"; then
  echo "Build information does not contain the expected source commit ${COMMIT}." >&2
  exit 1
fi

copy_release_asset deploy/config.example.yaml "${RELEASE_DIR}/config.yaml"
copy_release_asset deploy/config.example.yaml "${RELEASE_DIR}/config.example.yaml"
copy_release_asset deploy/install-centos7.sh "${RELEASE_DIR}/install-centos7.sh"
copy_release_asset deploy/uninstall-centos7.sh "${RELEASE_DIR}/uninstall-centos7.sh"
copy_release_asset deploy/traffic-go.service "${RELEASE_DIR}/traffic-go.service"

cat >"${RELEASE_DIR}/release-manifest.json" <<EOF
{
  "name": "${APP_NAME}",
  "version": "${VERSION_NUMBER}",
  "git_commit": "${COMMIT}",
  "git_tag": "${EXPECTED_TAG}",
  "source_date_epoch": ${SOURCE_DATE_EPOCH},
  "build_date": "${BUILD_DATE}",
  "go_version": "${GO_VERSION}",
  "node_version": "${NODE_VERSION}",
  "npm_version": "${NPM_VERSION}",
  "target": "linux/amd64",
  "cgo_enabled": false,
  "archive": "${ARCHIVE_NAME}",
  "dirty": $([[ -n "${DIRTY_STATUS}" ]] && printf true || printf false)
}
EOF
"${NODE_BIN}" -e "JSON.parse(require('fs').readFileSync(process.argv[1], 'utf8'))" "${RELEASE_DIR}/release-manifest.json"

cat >"${RELEASE_DIR}/SBOM-go.spdx" <<EOF
SPDXVersion: SPDX-2.3
DataLicense: CC0-1.0
SPDXID: SPDXRef-DOCUMENT
DocumentName: ${APP_NAME}-${VERSION_NUMBER}
DocumentNamespace: https://traffic-go.invalid/spdx/${VERSION_NUMBER}/${COMMIT}
Creator: Tool: deploy/package-release.sh
Created: ${BUILD_DATE}

PackageName: ${APP_NAME}
SPDXID: SPDXRef-Package-traffic-go
PackageVersion: ${VERSION_NUMBER}
PackageDownloadLocation: NOASSERTION
FilesAnalyzed: false
PackageLicenseConcluded: NOASSERTION
PackageLicenseDeclared: NOASSERTION
PackageCopyrightText: NOASSERTION
Relationship: SPDXRef-DOCUMENT DESCRIBES SPDXRef-Package-traffic-go
EOF

MODULE_INDEX=0
while read -r module module_version _; do
  [[ -n "${module}" ]] || continue
  [[ "${module}" == "traffic-go" ]] && continue
  MODULE_INDEX=$((MODULE_INDEX + 1))
  module_version="${module_version:-NOASSERTION}"
  cat >>"${RELEASE_DIR}/SBOM-go.spdx" <<EOF

PackageName: ${module}
SPDXID: SPDXRef-GoModule-${MODULE_INDEX}
PackageVersion: ${module_version}
PackageDownloadLocation: NOASSERTION
FilesAnalyzed: false
PackageLicenseConcluded: NOASSERTION
PackageLicenseDeclared: NOASSERTION
PackageCopyrightText: NOASSERTION
Relationship: SPDXRef-Package-traffic-go DEPENDS_ON SPDXRef-GoModule-${MODULE_INDEX}
EOF
done < <(cd "${BUILD_ROOT}" && run_go list -m all)

run_npm --prefix "${BUILD_WEB_ROOT}" ls --omit=dev --all --json >"${RELEASE_DIR}/SBOM-web-production.json"

verify_source_state
verify_build_source_state
find "${RELEASE_DIR}" -type d -exec chmod 0755 {} +
find "${RELEASE_DIR}" -type f -exec chmod 0644 {} +
chmod 0755 "${RELEASE_DIR}/${APP_NAME}" "${RELEASE_DIR}/install-centos7.sh" "${RELEASE_DIR}/uninstall-centos7.sh"

(
  cd "${RELEASE_DIR}"
  sha256sum \
    "${APP_NAME}" \
    BUILD-INFO.txt \
    SBOM-go.spdx \
    SBOM-web-production.json \
    config.yaml \
    config.example.yaml \
    install-centos7.sh \
    release-manifest.json \
    traffic-go.service \
    uninstall-centos7.sh >SHA256SUMS
  chmod 0644 SHA256SUMS
)

tar \
  --sort=name \
  --mtime="@${SOURCE_DATE_EPOCH}" \
  --owner=0 \
  --group=0 \
  --numeric-owner \
  -C "${RELEASE_DIR}" \
  -cf - . | gzip -n >"${ARCHIVE_PATH}"

(
  cd "${STAGING_DIR}"
  sha256sum "${ARCHIVE_NAME}" >"${ARCHIVE_NAME}.sha256"
)

# Publish the directory, archive, checksum, and manifests with one same-filesystem
# rename. A failed build leaves every previous version untouched.
verify_source_state
verify_build_source_state
verify_release_base
chmod 0755 "${STAGING_DIR}" "${RELEASE_DIR}"
if [[ -e "${FINAL_VERSION_ROOT}" || -L "${FINAL_VERSION_ROOT}" ]]; then
  echo "Versioned output appeared during the build and will not be changed: ${FINAL_VERSION_ROOT}" >&2
  exit 1
fi
mv -n -T -- "${STAGING_DIR}" "${FINAL_VERSION_ROOT}"
if [[ -d "${STAGING_DIR}" ]]; then
  echo "Atomic no-replace publish refused an existing destination: ${FINAL_VERSION_ROOT}" >&2
  exit 1
fi
STAGING_DIR=""
STAGING_OWNED=0
rmdir -- "${LOCK_DIR}"
LOCK_HELD=0
LOCK_DIR=""

cat <<EOF
Build complete.
Version: ${VERSION_NUMBER}
Commit: ${COMMIT}
Output: ${FINAL_RELEASE_DIR}
Archive: ${FINAL_ARCHIVE_PATH}
Checksum: ${FINAL_ARCHIVE_CHECKSUM}
EOF
