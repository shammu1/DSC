import subprocess
import json
import sys
from pathlib import Path

# robust adapter import
try:
    # Correct relative import: from resources.apt.resources.apt -> up 2 levels -> resources.apt
    from ...adapters.python.adapter import resource_adapter as adapter  # package relative
except Exception:
    _here = Path(__file__).resolve()
    # Path tree: .../DSC/resources/apt/resources/apt/AptPackage.py
    # parents[4] -> repo root (DSC), parents[3] -> top-level 'resources'
    _repo_root = _here.parents[4] if len(_here.parents) >= 5 else _here.parent
    _resources_root = _here.parents[3] if len(_here.parents) >= 4 else _here.parent
    for p in (_repo_root, _resources_root):
        p_str = str(p)
        if p_str not in sys.path:
            sys.path.insert(0, p_str)
    try:
        from adapters.python.adapter import resource_adapter as adapter  # absolute import
    except Exception:
        from contextlib import contextmanager
        class _FallbackAdapter:  # type: ignore
            @contextmanager
            def profile_block(self, label):
                yield
            def log(self, level, message: str, target: str = None, **kwargs):
                print(json.dumps({"level": level, "message": message + "From Exception", "target": target, **kwargs}), file=sys.stderr)
        adapter = _FallbackAdapter()


class AptPackage:
    """
    This class provides methods to manage APT packages on a Linux system.
    It includes methods to install, remove, and check the status of packages.
    """
    def __init__(self, name, version=None, _exist=True, source=None, dependencies=[]):
        self.name = name
        self.version = version
        self._exist = _exist
        self.source = source
        self.dependencies = dependencies

    @classmethod
    def from_json(cls, json_str: str, operation: str = None) -> 'AptPackage':
        # Pass operation so conditional validation can occur
        if not adapter.validate_input_json(json_str, operation=operation):
            raise ValueError("Invalid JSON input")
        data = json.loads(json_str)
        return AptPackage(
            name=data.get('name'),
            version=data.get('version'),
            _exist=data.get('_exist', True),
            source=data.get('source'),
            dependencies=data.get('dependencies') or []
        )

    # @classmethod
    # def from_json(cls, json_str: str) -> 'AptPackage':
    #     """Create an AptPackage object from JSON string."""
    #     if not adapter.validate_input_json(json_str):
    #         raise ValueError("Invalid JSON input")
    #     data = json.loads(json_str)
    #     return AptPackage(
    #         name=data.get('name'),
    #         version=data.get('version'),
    #         _exist=data.get('_exist', True),
    #         source=data.get('source'),
    #         dependencies=data.get('dependencies') or []
    #     )

    def to_json(self) -> str:
        """Create an JSON string representation of package instance."""
        pkg_data = {
            "name": self.name,
            "version": self.version,
            "_exist": self._exist,
            "source": self.source,
            "dependencies": self.dependencies
        }
        return json.dumps(pkg_data)

    def installed_pkg_versions(self):
        try:
            installed_version = []
            installed_packages = subprocess.run(['dpkg', '-l', self.name], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            for line in installed_packages.stdout.splitlines():
                if line.startswith('ii') and self.name in line:
                    version = line.split()[2]
                    installed_version.append(version)
            return installed_version
        except subprocess.CalledProcessError as err:
            adapter.log("error",f"Error fetching installed versions for {self.name}: {err}", "Apt Management", method="installed_pkg_versions")
            return []

    def get_latest_installed_version(self):
        try:
            result = subprocess.run(['dpkg', '-l', self.name], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            versions = []
            for line in result.stdout.splitlines():
                if line.startswith('ii') and self.name in line:
                    version = line.split()[2]
                    versions.append(version)
            if versions:
                return sorted(versions)[-1]
            else:
                return None
        except Exception as err:
            adapter.log("error",f"Error fetching latest of versions installed for {self.name}: {err}", "Apt Management", method="get_latest_installed_version")
            return None

    def get_all_available_versions(self):
        """Get all available versions of a package."""
        try:
            available_versions = subprocess.run(['apt-cache', 'madison', self.name], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
            return [line.split('|')[1].strip() for line in available_versions.stdout.splitlines() if '|' in line]
        except subprocess.CalledProcessError as err:
            adapter.log("error", f"Error fetching available versions for {self.name}: {err}", "Apt Management", method="get_all_available_versions")
            return []

    def is_installed(self):
        """Check if the specified APT package is installed."""
        try:
            if self.version:
                return (self.version in self.installed_pkg_versions())
            else:
                return len(self.installed_pkg_versions()) > 0
                # installed = subprocess.run(['dpkg', '-l', self.name], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                # if installed.returncode == 0:
                #     return True
                # else:
                #     return False
        except Exception as err:
            adapter.log("error", f"Error checking package '{self.name}': {err}", "Apt Management", method="is_installed")
            return False

    def get(self):
        """Return the current state of the package as a JSON string."""
        with adapter.profile_block("DSC Get Operation"):
            installed = self.is_installed()
            version = self.version
            if installed and not version:
                version = self.get_latest_installed_version()
            # version = self.get_latest_installed_version() if not self.version else self.version
            # is_installed = self.is_installed() if version is not None else False
            pkg_dict = {
                "name": self.name,
                "version": version,
                "_exist": installed, #is_installed,
                "source": self.source,
                "dependencies": self.dependencies
            }
            adapter.log("trace","Get Status for Apt - Test1", "Apt Management", command="get", method="get")
        return pkg_dict

    def install(self):
        """Install the specified APT package."""
        try:
            subprocess.run(['sudo', 'apt-get', 'install', '-y', self.name], check=True)
        except subprocess.CalledProcessError as err:
            adapter.log("error", f"Failed to install package '{self.name}': {err}", "Apt Management", command="set", method="install")
            pass

    def delete(self):
        """Remove the specified APT package."""
        try:
            subprocess.run(['sudo', 'apt-get', 'remove', '-y', self.name], check=True)
        except subprocess.CalledProcessError as err:
            adapter.log("error", f"Failed to remove package '{self.name}': {err}", "Apt Management", command="set", method="delete")

    def test(self):
        """ Test if the state of an APT package aligns with its configuration """
        try:
            actual_state = self.get()
            in_desired_state = self._exist == actual_state["_exist"]

            differingProperties = []
            if not in_desired_state:
                differingProperties.append("_exist")

            return actual_state, differingProperties

        except subprocess.CalledProcessError as err:
            adapter.log("error",f"Failed to test state for package '{self.name}': {err}", "Apt Management", command="test", method="test")
            return {"error": f"Failed to test state: {err}"}, []

    def set(self):
        """Install/Uninstall as needed in the configuration"""
        try:
            before_installed = self.is_installed()
            if self._exist and not before_installed:
                self.install()
            elif not self._exist and before_installed:
                self.delete()

            after_installed = self.is_installed()
            diffs = []
            if before_installed != after_installed:
                diffs.append("_exist")

            return {
                "_exist": after_installed,
                "differingProperties": diffs
            }
        except subprocess.CalledProcessError as err:
            adapter.log("error", f"Failed to set state for package '{self.name}': {err}", "Apt Management", command="set", method="set")
            return f"Failed to set state: {err}"

    @staticmethod
    def export(apt_package=None):
        """Export a list of all installed APT packages."""
        try:
            if apt_package:
                available_check = subprocess.run(
                    ['apt-cache', 'show', apt_package.name],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                if available_check.returncode != 0:
                    adapter.log_error(f"Package provided in the config cannot be installed.", "Apt Management", command="export", method="export")
                    sys.exit(1)

            dpkg_output = subprocess.check_output(['dpkg-query', '-W', '-f=${Package}\n']).decode().splitlines()
            packages = []
            for pkg in dpkg_output:
                try:
                    apt_cache_output = subprocess.check_output(['apt-cache', 'show', pkg]).decode()
                    pkg_info = {}
                    for line in apt_cache_output.splitlines():
                        if line.startswith('Package:'):
                            pkg_info['name'] = line.split(':', 1)[1].strip()
                        elif line.startswith('Version:'):
                            pkg_info['version'] = line.split(':', 1)[1].strip()
                        elif line.startswith('Depends:'):
                            raw_deps = line.split(':', 1)[1].strip()
                            pkg_info['dependencies'] = [d.strip() for d in raw_deps.split(',') if d.strip()]
                        elif line.startswith('Description:'):
                            pkg_info['description'] = line.split(':', 1)[1].strip()

                    try:
                        source_info = subprocess.run(
                            ['apt-cache', 'policy', pkg_info['name']],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            text=True,
                            check=False
                        )
                        lines = source_info.stdout.splitlines()
                        installed_urls = []
                        in_installed = False
                        for raw_line in lines:
                            line = raw_line.rstrip('\n')
                            stripped = line.strip()
                            if stripped.startswith('***'):
                                in_installed = True
                                continue
                            if not in_installed:
                                continue
                            if stripped == '':
                                break
                            if (not line.startswith(' ') and
                                not stripped.startswith('***') and
                                '/var/lib/dpkg/status' not in stripped and
                                not stripped.startswith('500 ') and
                                not stripped[0].isdigit()):
                                break
                            if '/var/lib/dpkg/status' in stripped:
                                continue
                            tokens = stripped.split()
                            if not tokens:
                                continue
                            url_token = None
                            for t in tokens:
                                if t.startswith('http://') or t.startswith('https://'):
                                    url_token = t.rstrip('/')
                                    break
                            if url_token and url_token not in installed_urls:
                                installed_urls.append(url_token)
                        if installed_urls:
                            pkg_info['source'] = installed_urls[0]
                            pkg_info['sourceRepos'] = installed_urls
                        else:
                            pkg_info['source'] = "unknown"
                    except Exception:
                        pkg_info['source'] = "unknown"

                    pkg_info['_exist'] = True

                    if apt_package:
                        if apt_package.name != pkg_info.get('name'):
                            continue
                        if apt_package.version not in (pkg_info['version'], None):
                            continue
                        if apt_package.source not in (pkg_info['source'], None):
                            continue
                        if apt_package.dependencies not in (pkg_info.get('dependencies', []), None):
                            continue
                    if pkg_info:
                        packages.append(pkg_info)

                except subprocess.CalledProcessError:
                    continue

            if apt_package and not packages:
                adapter.log("error", f"Package provided in the config is not currently installed.", "Apt Management", command="export", method="export")
                sys.exit(1)

            result = {"packages": packages}
            print(json.dumps(result))
        except Exception as err:
            adapter.log("error", f"Failed to export packages: {err}", "Apt Management", command="export", method="export")
            print(json.dumps({"error": str(err), "packages": []}))
            return {'Error': str(err)}


