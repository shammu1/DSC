import subprocess
import json
import sys
from logger import dfl_logger as Logger

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
        return cls(
            name = data.get('name'),
            version = data.get('version'),
            _exist = data.get('_exist'),
            source = data.get('source'),
            dependencies = data.get('dependencies')
        )

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
            Logger.error(f"Error fetching installed versions for {self.name}: {err}", "Apt Management", method="installed_pkg_versions");
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
            Logger.error(f"Error fetching latest of versions installed for {self.name}: {err}", "Apt Management",method = "get_latest_installed_version");
            return None


    def get_all_available_versions(self):
        """Get all available versions of a package."""
        try:
            available_versions = subprocess.run(['apt-cache', 'madison', self.name], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
            return [line.split('|')[1].strip() for line in available_versions.stdout.splitlines() if '|' in line]
        except subprocess.CalledProcessError as err:
            Logger.error(f"Error fetching available versions for {self.name}: {err}", "Apt Management", method = "get_all_available_versions");
        return []


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
        Logger.info("Get Status for Apt", "Apt Management", command="get", method = "get");
        return json.dumps(pkg_dict)

    def install(self):
        """Install the specified APT package."""
        try:
            subprocess.run(['sudo', 'apt-get', 'install', '-y', self.name], check=True)
            #self._exist = True
            #print(f"Package '{self.name}' installed successfully.")
        except subprocess.CalledProcessError as err:
            Logger.error(f"Failed to install package '{self.name}': {err}", "Apt Management", command="set", method="install");

    def delete(self):
        """Remove the specified APT package."""
        try:
            subprocess.run(['sudo', 'apt-get', 'remove', '-y', self.name], check=True)
            #self._exist = False
            #print(f"Package '{self.name}' removed successfully.")
        except subprocess.CalledProcessError as err:
            Logger.error(f"Failed to remove package '{self.name}': {err}", "Apt Management", command="set", method="delete");


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
            Logger.error(f"Error checking package '{self.name}': {err}", "Apt Management", method = "is_installed");
            return False

    def test(self):
        """ Test if the state of an APT package aligns with its configuration """ 
        try:
            return not (self._exist and not self.is_installed()) or (not self._exist and self.is_installed())
        except subprocess.CalledProcessError as err:
            Logger.error(f"Failed to test state for package '{self.name}': {err}", "Apt Management", command = "test", method = "test");
            return f"Failed to test state: {err}"
    
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
            
            return json.dumps(set_dict) 
        except subprocess.CalledProcessError as err:
            Logger.error(f"Failed to set state for package '{self.name}': {err}", "Apt Management", command = "set", method = "set");
            return f"Failed to set state: {err}"

    @staticmethod
    def export(apt_package=None):
        """Export a list of all installed APT packages."""
        try:
            sys.stderr.write(f"Export Package : {apt_package is None}")
            package_list = subprocess.run(
                ['dpkg-query', '-W', '-f=${Package} ${Version} ${Description} ${Depends}\n'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            filtered_packages = ""
            for line in package_list.stdout.splitlines():
                if not line.strip():
                    continue
                parts = line.split(None, 3) 
                if len(parts) < 3:
                    continue  
                if len(parts) == 3:
                    package, version, description = parts
                    depends = ""
                else:
                    package, version, description, depends = parts

                if apt_package:
                    if apt_package.name != package:
                        continue
                    if apt_package.version not in (version,None):
                        continue
                pkg_dict = {
                        "name": package,
                        "version": version,
                        "description": description,
                        "dependencies": depends
                    }
                if filtered_packages != "":
                    filtered_packages += '\n'
                filtered_packages += json.dumps(pkg_dict)

            return {"Packages List" : filtered_packages if filtered_packages != "" else "No installed packages for the provided Export Criteria!"} 

        except Exception as err:
            Logger.error(f"Failed to export packages: {err}", "Apt Management", command ="export", method = "test");
            return {'Error': str(err)}

