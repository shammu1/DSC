import subprocess
import json
import sys
from typing import Any, Dict, List, Optional, Tuple, Callable
from pathlib import Path
import logging
import io
import time
log = logging.getLogger(__name__)

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
        # TODO: Placeholder for any input validations if needed
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
            #adapter.log("error",f"Error fetching installed versions for {self.name}: {err}", "Apt Management", method="installed_pkg_versions")
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
            #adapter.log("error",f"Error fetching latest of versions installed for {self.name}: {err}", "Apt Management", method="get_latest_installed_version")
            return None

    def get_all_available_versions(self):
        """Get all available versions of a package."""
        try:
            available_versions = subprocess.run(['apt-cache', 'madison', self.name], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
            return [line.split('|')[1].strip() for line in available_versions.stdout.splitlines() if '|' in line]
        except subprocess.CalledProcessError as err:
            #adapter.log("error", f"Error fetching available versions for {self.name}: {err}", "Apt Management", method="get_all_available_versions")
            return []

    def is_installed(self):
        """Check if the specified APT package is installed."""
        try:
            if self.version:
                return (self.version in self.installed_pkg_versions())
            else:
                return len(self.installed_pkg_versions()) > 0

        except Exception as err:
            #adapter.log("error", f"Error checking package '{self.name}': {err}", "Apt Management", method="is_installed")
            return False

    def get(self):
        """Return the current state of the package as a JSON string."""
        installed = self.is_installed()
        version = self.version
        if installed and not version:
            version = self.get_latest_installed_version()
            
            # Ensure dependencies is always a list (DSC-friendly)
        deps = self.dependencies if isinstance(self.dependencies, list) else []
            
        state = {
                    "name": self.name,
                    "_exist": bool(installed),
                    "dependencies": deps,
                }
    
        # Only include optional fields if they are valid types
        if version:
            state["version"] = version
            
        if isinstance(self.source, str) and self.source.strip():
           state["source"] = self.source

        #adapter.log("trace","Get Status for Apt - Test1", "Apt Management", command="get", method="get")
        log.debug("Computed GET state for '%s'", self.name)
        
        return state


    def install(self):
        """Install the specified APT package."""
        try:
            subprocess.run(['sudo', 'apt-get', 'install', '-y', self.name], check=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        except subprocess.CalledProcessError as err:
            #adapter.log("error", f"Failed to install package '{self.name}': {err}", "Apt Management", command="set", method="install")
            return

    def delete(self):
        """Remove the specified APT package."""
        try:
            subprocess.run(['sudo', 'apt-get', 'remove', '-y', self.name], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        except subprocess.CalledProcessError as err:
            #adapter.log("error", f"Failed to remove package '{self.name}': {err}", "Apt Management", command="set", method="delete")
            return
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
            # adapter.log("error",f"Failed to test state for package '{self.name}': {err}", "Apt Management", command="test", method="test")
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

            version = self.version
            if after_installed and not version:
                version = self.get_latest_installed_version()

            state = {
                "name": self.name,
                "_exist": after_installed
            }
            if version:
                state["version"] = version

            return state, diffs

        except subprocess.CalledProcessError as err:
            #adapter.log("error", f"Failed to set state for package '{self.name}': {err}", "Apt Management", command="set", method="set")
            return {
                "state": {
                    "name": self.name,
                    "_exist": before_installed if 'before_installed' in locals() else False
                },
                "differingProperties": ["_exist"]
            }

    @staticmethod
    def export(apt_package=None):
        """Export a list of all installed APT packages."""
        try:
            # If filtering, validate the requested package exists in apt-cache first
            if apt_package and apt_package.name:
                available_check = subprocess.run(
                ['apt-cache', 'show', apt_package.name],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
                )
                if available_check.returncode != 0:
                    # Package not available in apt repos; return empty, don't exit
                    return {"packages": []}

            # Get all installed packages
            dpkg_output = subprocess.check_output(['dpkg-query', '-W', '-f=${Package}\n']).decode().splitlines()
            packages = []
        
            for pkg in dpkg_output:
                # If filtering and this package doesn't match the requested name, skip it
                if apt_package and apt_package.name and pkg != apt_package.name:
                    continue

                try:
                    apt_cache_output = subprocess.check_output(['apt-cache', 'show', pkg]).decode()
                    pkg_info = {}
                    for line in apt_cache_output.splitlines():
                        if line.startswith('Package:'):
                            pkg_info['name'] = line.split(':', 1)[1].strip()
                        elif line.startswith('Version:'):
                            pkg_info['version'] = line.split(':', 1)[1].strip()
                        elif line.startswith('Depends:'):
                            pkg_info['dependencies'] = line.split(':', 1)[1].strip()
                        elif line.startswith('Description:'):
                            pkg_info['description'] = line.split(':', 1)[1].strip()
                    

                    # Apply additional filters if provided
                    if apt_package:
                        # Filter by version if specified
                        if apt_package.version and apt_package.version != pkg_info.get('version'):
                            continue
                        # Filter by source if specified
                        if apt_package.source and apt_package.source != pkg_info.get('source'):
                            continue
                        # Note: dependencies filter is complex; skip for now or implement carefully

                    pkg_info['_exist'] = True
                    packages.append(pkg_info)

                except subprocess.CalledProcessError:
                    continue

            # If filtering was requested but no packages matched, return empty
            if apt_package and not packages:
                return {"packages": []}

            return {"packages": packages}
        
        except Exception as err:
            return {'error': str(err)}