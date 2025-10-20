import subprocess
import json
import sys
from pathlib import Path

# robust adapter import
try:
    from ...adapter import ResourceAdapter  # package relative
except Exception:
    # fallback: add parent dirs to sys.path then absolute import
    _here = Path(__file__).resolve()
    _pkg_root = _here.parents[3] if len(_here.parents) >= 4 else _here.parent
    if str(_pkg_root) not in sys.path:
        sys.path.insert(0, str(_pkg_root))
    try:
        from resources.apt_python.adapter.adapter import resourceadapter
    except Exception as _imp_err:  # last fallback: minimal shim
        class ResourceAdapter:  # type: ignore
            @staticmethod
            def log_error(msg, *_, **__): print(json.dumps({"level":"error","message":msg}), file=sys.stderr)
            @staticmethod
            def log_info(msg, *_, **__): print(json.dumps({"level":"info","message":msg}), file=sys.stderr)


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
    def from_json(cls, json_str: str) -> 'AptPackage':
        """Create an AptPackage object from JSON string."""
        data = json.loads(json_str)
        return AptPackage(
            name=data.get('name'),
            version=data.get('version'),
            _exist=data.get('_exist', True),
            source=data.get('source'),
            dependencies=data.get('dependencies') or []
        )

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
            ResourceAdapter.log_error(f"Error fetching installed versions for {self.name}: {err}", "Apt Management", method="installed_pkg_versions")
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
            ResourceAdapter.log_error(f"Error fetching latest of versions installed for {self.name}: {err}", "Apt Management",method = "get_latest_installed_version")
            return None


    def get_all_available_versions(self):
        """Get all available versions of a package."""
        try:
            available_versions = subprocess.run(['apt-cache', 'madison', self.name], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
            return [line.split('|')[1].strip() for line in available_versions.stdout.splitlines() if '|' in line]
        except subprocess.CalledProcessError as err:
            ResourceAdapter.log_error(f"Error fetching available versions for {self.name}: {err}", "Apt Management", method = "get_all_available_versions")
            return []


    def is_installed(self):
        """Check if the specified APT package is installed."""
        try:
            if self.version:
                return (self.version in self.installed_pkg_versions())
            else:
                installed = subprocess.run(['dpkg', '-l', self.name], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                if installed.returncode == 0:
                    #print(f"Package '{self.name}' is installed.")
                    return True
                else:
                    #print(f"Package '{self.name}' is not installed.")
                    return False
        except Exception as err:
            ResourceAdapter.log_error(f"Error checking package '{self.name}': {err}", "Apt Management", method = "is_installed")
            return False

    def get(self):
        """Return the current state of the package as a JSON string."""
        version =  self.get_latest_installed_version() if not self.version else self.version
        is_installed = self.is_installed() if version is not None else False
        pkg_dict = {
            "name": self.name,
            "version": version,
            "_exist": is_installed,
            "source": self.source,
            "dependencies": self.dependencies
        }
        ResourceAdapter.log_info("Get Status for Apt", "Apt Management", command="get", method = "get")
        return pkg_dict

    def install(self):
        """Install the specified APT package."""
        try:
            subprocess.run(['sudo', 'apt-get', 'install', '-y', self.name], check=True)
            #self._exist = True
            #print(f"Package '{self.name}' installed successfully.")
        except subprocess.CalledProcessError as err:
            ResourceAdapter.log_error(f"Failed to install package '{self.name}': {err}", "Apt Management", command="set", method="install")
            pass

    def delete(self):
        """Remove the specified APT package."""
        try:
            subprocess.run(['sudo', 'apt-get', 'remove', '-y', self.name], check=True)
            #self._exist = False
            #print(f"Package '{self.name}' removed successfully.")
        except subprocess.CalledProcessError as err:
            ResourceAdapter.log_error(f"Failed to remove package '{self.name}': {err}", "Apt Management", command="set", method="delete")

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
            ResourceAdapter.log_error(f"Failed to test state for package '{self.name}': {err}", "Apt Management", command="test", method="test")
            return {"error": f"Failed to test state: {err}"}, []
    
    def set(self):
        """Install/Uninstall as needed in the configuration"""
        try:
            if self._exist and not self.is_installed():
                self.install()
            elif not self._exist and self.is_installed():
                self.delete()

            set_dict = {
            "_exist": self._exist
            }
            ResourceAdapter.log_info(f"Set state for package '{self.name}': {set_dict}", "Apt Management", command="set", method="set")
            return set_dict
        except subprocess.CalledProcessError as err:
            ResourceAdapter.log_error(f"Failed to set state for package '{self.name}': {err}", "Apt Management", command="set", method="set")
            return f"Failed to set state: {err}"


    @staticmethod
    def export(apt_package=None):
        """Export a list of all installed APT packages."""
        try:
            # If a specific package is requested, check if it exists in repositories
            if apt_package:
                # First check if the package is available in apt repositories
                available_check = subprocess.run(
                    ['apt-cache', 'show', apt_package.name], 
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE, 
                    text=True
                )
            
                # If command returns non-zero, package doesn't exist in repositories
                if available_check.returncode != 0:
                    ResourceAdapter.log_error(f"Package provided in the config cannot be installed.", "Apt Management", command="export", method="export")
                    sys.exit(1)
            
            
            dpkg_output = subprocess.check_output(['dpkg-query', '-W', '-f=${Package}\n']).decode().splitlines()
            
            # Initialize array to collect packages
            packages = []
        
            for pkg in dpkg_output:
                try:
                    # Get detailed info for each package
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
                        for idx, raw_line in enumerate(lines):
                            line = raw_line.rstrip('\n')
                            stripped = line.strip()

                            # Detect installed version marker
                            if stripped.startswith('***'):
                                in_installed = True
                                continue

                            if not in_installed:
                                continue

                            # End conditions for the installed block:
                            # - blank line
                            # - a non-indented new version header (starts with a digit or '*')
                            if stripped == '':
                                break
                            # A new version header (not indented and not '/var/lib/dpkg/status')
                            if (not line.startswith(' ') and
                                not stripped.startswith('***') and
                                '/var/lib/dpkg/status' not in stripped and
                                not stripped.startswith('500 ') and
                                not stripped[0].isdigit()):  # conservative break
                                break

                            # Skip local status
                            if '/var/lib/dpkg/status' in stripped:
                                continue
                            
                            # Repository lines are indented and contain a priority then a URL
                            # Examples:
                            # "     500 http://archive.ubuntu.com/ubuntu noble-updates/main amd64 Packages"
                            # "     100 /var/lib/dpkg/status"
                            # We only keep lines that have an http(s) token.
                            tokens = stripped.split()
                            if not tokens:
                                continue

                            # Find first http(s) token
                            url_token = None
                            for t in tokens:
                                if t.startswith('http://') or t.startswith('https://'):
                                    url_token = t.rstrip('/')
                                    break
                            if url_token and url_token not in installed_urls:
                                installed_urls.append(url_token)

                        if installed_urls:
                            # Store first (primary) URL; optionally keep all in a list field
                            pkg_info['source'] = installed_urls[0]
                            pkg_info['sourceRepos'] = installed_urls
                        else:
                            pkg_info['source'] = "unknown"
                    except Exception:
                        pkg_info['source'] = "unknown"


                    pkg_info['_exist'] = True
                    #pkg_info['installable'] = True

                    # Apply filter if apt_package is provided
                    if apt_package:
                        if apt_package.name != pkg_info.get('name'):
                            continue
                        if apt_package.version not in (pkg_info['version'],None):
                            continue
                        if apt_package.source not in (pkg_info['source'],None):
                            continue
                        if apt_package.dependencies not in (pkg_info['dependencies'],None):
                            continue
                    if pkg_info:
                        packages.append(pkg_info)

                except subprocess.CalledProcessError:
                    continue 

            # Handle case where apt_package is provided but no matching packages found
            if apt_package and not packages:
                ResourceAdapter.log_error(f"Package provided in the config is not currently installed.", "Apt Management", command="export", method="export")        
                sys.exit(1)

            result = {"packages": packages}
            print(json.dumps(result))
        except Exception as err:
            ResourceAdapter.log_error(f"Failed to export packages: {err}", "Apt Management", command="export", method="export")
            print(json.dumps({"error": str(err), "packages": []}))
            return {'Error': str(err)}


