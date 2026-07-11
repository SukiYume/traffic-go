package deploy_test

import (
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func readScript(t *testing.T, name string) string {
	t.Helper()
	content, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(content)
}

func TestWorkflowYAMLParses(t *testing.T) {
	for _, name := range []string{"../.github/workflows/ci.yml", "../.github/workflows/release.yml", "../.github/dependabot.yml"} {
		t.Run(name, func(t *testing.T) {
			content, err := os.ReadFile(name)
			if err != nil {
				t.Fatalf("read workflow: %v", err)
			}
			var document map[string]any
			if err := yaml.Unmarshal(content, &document); err != nil {
				t.Fatalf("parse workflow: %v", err)
			}
			if len(document) == 0 {
				t.Fatal("workflow is empty")
			}
		})
	}
}

func TestShellScriptsParse(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("bash syntax validation runs in Linux CI")
	}
	for _, name := range []string{"install-centos7.sh", "uninstall-centos7.sh", "package-release.sh", "build-linux-gitbash.sh", "clean-generated.sh"} {
		t.Run(name, func(t *testing.T) {
			command := exec.Command("bash", "-n", name)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("bash -n %s: %v\n%s", name, err, output)
			}
		})
	}
}

func TestGeneratedCleanupSafetyContract(t *testing.T) {
	script := readScript(t, "clean-generated.sh")
	for _, required := range []string{
		`[[ -L "${current}" ]]`,
		`git -C "${ROOT_DIR}" ls-files`,
		`git -C "${ROOT_DIR}" check-ignore`,
		`mountpoint -q`,
		`case "${TARGET}" in`,
		`rm -rf -- "${TARGET}"`,
	} {
		if !strings.Contains(script, required) {
			t.Errorf("generated cleanup helper is missing %q", required)
		}
	}
	if runtime.GOOS == "windows" {
		return
	}
	command := exec.Command("bash", "clean-generated.sh", "--check", "../deploy")
	if output, err := command.CombinedOutput(); err == nil {
		t.Fatalf("cleanup safety check accepted traversal into tracked source:\n%s", output)
	}
	command = exec.Command("bash", "clean-generated.sh", "--check", "deploy")
	if output, err := command.CombinedOutput(); err == nil {
		t.Fatalf("cleanup safety check accepted tracked source:\n%s", output)
	}
	command = exec.Command("bash", "clean-generated.sh", "--check", "tmp/clean-contract-probe")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("cleanup safety check rejected ignored generated path: %v\n%s", err, output)
	}
}

func TestMakefileDelegatesDestructiveCleanup(t *testing.T) {
	makefile := readScript(t, "../Makefile")
	if count := strings.Count(makefile, `bash deploy/clean-generated.sh`); count != 3 {
		t.Fatalf("Makefile must delegate all three clean targets to the safety helper, found %d", count)
	}
	for _, unsafe := range []string{`rm -rf -- "$(BUILD_DIR)"`, `rm -rf -- "$(RELEASE_ROOT)"`, `rm -rf -- "$(WEB_DIR)/dist"`} {
		if strings.Contains(makefile, unsafe) {
			t.Fatalf("Makefile still contains unsafe direct cleanup %q", unsafe)
		}
	}
	if !strings.Contains(makefile, `$(if $(filter dev,$(VERSION)),,VERSION=$(VERSION))`) {
		t.Fatal("make release-linux must forward an explicitly selected VERSION")
	}
}

func TestInstallerHasSafeUpgradeContract(t *testing.T) {
	script := readScript(t, "install-centos7.sh")
	for _, required := range []string{
		"systemctl restart traffic-go.service",
		"/api/v1/readyz",
		"systemd-analyze verify",
		"/etc/modules-load.d/traffic-go.conf",
		"/etc/sysctl.d/99-traffic-go.conf",
		"restoring the previous traffic-go files",
		"STATE_DIR=",
		"chmod 0750 \"${INSTALL_ROOT}\"",
		"chmod 0700 \"${STATE_DIR}\"",
		"readlink -m",
		"resolves to a protected system directory",
		"must use a dedicated leaf directory",
		".traffic-go-managed-install",
		"Existing managed ${label} must not be a symlink",
		"without an explicit inactive result",
		"Both legacy and state-directory SQLite files exist",
		"sha256sum --strict --check SHA256SUMS",
		"TRAFFIC_GO_ALLOW_UNVERIFIED_PACKAGE=1",
		"state_dir=%s\\ndb_path=%s",
		"AUTHORITY_FILE=\"/etc/traffic-go-install-layout\"",
		"DEPLOY_LOCK=\"/run/traffic-go-install.lock\"",
		"Another traffic-go install or uninstall transaction is already running",
		"require_secure_parent_chain \"Install root\"",
		"root-owned and not writable through mode bits or ACLs",
		"cp -a --reflink=auto",
		"Another traffic-go layout is already authoritative",
		"Processes for the dedicated ${RUN_USER} account remain",
		"chown root:root \"${STATE_DIR}\"",
		"restore_database_artifacts",
		"Rollback database snapshot is missing or unsafe",
		"Managed file rollback was not started because one or more backups failed preflight",
		"MANAGED_FILES_MUTATED=1",
		"STATE_MARKER_BACKUP_NAME=\"state-marker-frozen\"",
		"consume_all_temporary_files",
		"leaving traffic-go.service stopped to protect the database",
	} {
		if !strings.Contains(script, required) {
			t.Errorf("installer is missing %q", required)
		}
	}
	if strings.Contains(script, "RUN_USER=\"root\"") {
		t.Fatal("installer must not silently switch the service to root")
	}
	if strings.Contains(script, "usermod ") {
		t.Fatal("installer must validate rather than rewrite an existing service account")
	}
	if !strings.Contains(script, `if [[ "${WAS_ACTIVE}" -eq 1 && "${rollback_failed}" -eq 0 ]]; then`) {
		t.Fatal("installer must not restart the old service after an incomplete rollback")
	}
}

func TestUninstallerPreservesDataByDefault(t *testing.T) {
	script := readScript(t, "uninstall-centos7.sh")
	for _, required := range []string{"PURGE_DATA=0", "--purge-data", "Type PURGE", "cancelling the entire uninstall", "db_path=*", "without an explicit inactive result", ".traffic-go-managed-state", "Refusing to uninstall without a root-owned regular", "environment path overrides are not accepted", "must not contain a symlink component", "not authoritative for the currently registered", "WorkingDirectory=${STATE_DIR}", "ExecStart=${SCRIPT_DIR}/traffic-go", "DEPLOY_LOCK=\"/run/traffic-go-install.lock\"", "Another traffic-go install or uninstall transaction is already running", "require_secure_parent_chain \"State directory\"", "Install authority changed while stopping the service", "preflight_file_removals", "STATE_METADATA_FROZEN=1", "not writable through mode bits or ACLs", "Default uninstall requires the state directory to be owned", "PROCESS_UIDS=(", "DELETION_STARTED=1", "Failed to restore the original traffic-go service state", "Unable to restrict retained file permissions"} {
		if !strings.Contains(script, required) {
			t.Errorf("uninstaller is missing %q", required)
		}
	}
	if strings.Contains(script, "rm -rf") {
		t.Fatal("uninstaller must not recursively remove the install directory")
	}
	preflightIndex := strings.Index(script, `preflight_file_removals "${removal_targets[@]}"`)
	disableIndex := strings.Index(script, `systemctl disable traffic-go.service >/dev/null`)
	deletionIndex := strings.Index(script, "DELETION_STARTED=1")
	if preflightIndex < 0 || disableIndex < preflightIndex || deletionIndex < disableIndex {
		t.Fatal("uninstaller must finish preflight before disabling the service or starting deletion")
	}
}

func TestReleaseScriptCarriesProvenanceAndIntegrity(t *testing.T) {
	script := readScript(t, "package-release.sh")
	for _, required := range []string{
		"APP_NAME must be a single safe basename",
		"Release version must be a safe SemVer value",
		"git -C \"${ROOT_DIR}\" status --porcelain",
		"git -C \"${ROOT_DIR}\" describe --tags --exact-match",
		"-trimpath",
		"traffic-go/internal/buildinfo.Version",
		"release-manifest.json",
		"SBOM-go.spdx",
		"SHA256SUMS",
		"gzip -n",
		"mktemp -d",
		"FINAL_VERSION_ROOT",
		"Another build or a stale release lock exists",
		"mv -n -T --",
		"Built binary provenance mismatch",
		"Built binary build ID mismatch",
		"-buildvcs=false",
		"source.commit:",
		"Release path must not contain symlink components",
		"Source HEAD or worktree state changed during release packaging",
		"copy_release_asset",
		"umask 022",
		"export LC_ALL=C",
		"worktree add --detach",
		"core.hooksPath=",
		"verify_pristine_build_source",
		"verify_build_source_state",
		"PINNED_NODE_VERSION",
		"PINNED_NPM_VERSION",
		`"node_version": "${NODE_VERSION}"`,
		`"npm_version": "${NPM_VERSION}"`,
		"! -name placeholder.txt",
		"Tracked embedded placeholder is missing or unsafe",
		"HOST_VERSION_PROBE",
		`"${GO_HOST_OS}/${GO_HOST_ARCH}" == linux/amd64`,
	} {
		if !strings.Contains(script, required) {
			t.Errorf("release script is missing %q", required)
		}
	}
	if strings.Contains(script, `rm -rf -- "${RELEASE_DIR}"`) {
		t.Fatal("release script must not delete the previous successful output before validation")
	}
	if strings.Contains(script, "pushd") || strings.Contains(script, "popd") {
		t.Fatal("release script must use scoped working directories so error paths cannot unbalance directory state")
	}
}

func TestGitBashReleaseWrapperPrefersRepositoryToolchain(t *testing.T) {
	script := readScript(t, "build-linux-gitbash.sh")
	localToolchain := strings.Index(script, `.tools/go/bin/go.exe`)
	systemToolchain := strings.Index(script, `command -v go`)
	if localToolchain < 0 || systemToolchain < 0 || localToolchain > systemToolchain {
		t.Fatal("Git Bash release wrapper must prefer the repository-local Go toolchain")
	}
	for _, required := range []string{`[[ -n "${GO:-}" ]]`, `[[ -n "${NPM:-}" ]]`, `GOTOOLCHAIN="${GOTOOLCHAIN:-go1.26.5+auto}"`} {
		if !strings.Contains(script, required) {
			t.Errorf("Git Bash release wrapper is missing %q", required)
		}
	}
}

func TestServiceDoesNotMutateKernelAtRuntime(t *testing.T) {
	unit := readScript(t, "traffic-go.service")
	if strings.Contains(unit, "ExecStartPre") || strings.Contains(unit, "CAP_SYS_MODULE") {
		t.Fatal("service must rely on installer-managed persistent kernel prerequisites")
	}
	for _, required := range []string{"WorkingDirectory=__STATE_DIR__", "NoNewPrivileges=true", "AmbientCapabilities=", "ProtectSystem=strict", "ReadWritePaths=__STATE_DIR__", "ProtectKernelModules=true", "RestrictAddressFamilies=", "UMask=0077"} {
		if !strings.Contains(unit, required) {
			t.Errorf("service is missing %q", required)
		}
	}
}
