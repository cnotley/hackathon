"""
GRT-hackathon-team8-mcp packaging setup.

For more details on how to operate this file, check
https://w.amazon.com/index.php/Python/Brazil
"""

import os
import shutil
import subprocess

import pkg_resources
from setuptools import Command, setup

# Find toolbox supported OS: https://docs.hub.amazon.dev/builder-toolbox/user-guide/vending-repositories/#index-file
TOOLBOX_SUPPORTED_OS = ["alinux", "alinux_aarch64", "osx", "osx_arm64", "ubuntu", "windows"]


def run_command(command):
    """Run a command and return its output in decoded and stripped format."""
    return subprocess.check_output(command).decode().strip()


class ToolboxBundlerCommand(Command):

    description = "Bundle the project for toolbox"
    user_options = [
        ("toolbox-os=", "o", "Toolbox supported operating system."),
        ("publish", "p", "Publish to repository. Default is false."),
        ("repository-account=", "a", "Publish to repository. Default is false."),
        ("repository-role=", "r", "Repository account role name."),
        ("repository-name=", "b", "Repository name."),
        ("channel=", "c", "Repository channel name."),
    ]

    def initialize_options(self):
        self.toolbox_os = None
        self.publish = False
        self.repository_account = None
        self.repository_role = "toolbox-publish-role"
        self.repository_name = None
        self.channel = None

    def finalize_options(self):
        if self.toolbox_os is None:
            raise ValueError("--toolbox-os is required")
        if self.toolbox_os not in TOOLBOX_SUPPORTED_OS:
            raise ValueError(f"--toolbox-os must be one of: {TOOLBOX_SUPPORTED_OS}")
        if self.publish:
            if self.repository_account is None:
                raise ValueError("--repository-account is required")
            if self.repository_role is None:
                raise ValueError("--repository-role is required")
            if self.repository_name is None:
                raise ValueError("--repository-name is required")
            if self.channel is None:
                raise ValueError("--channel is required")

    def run(self):
        """Run the bundling command: you can only bundle for one platform_arch at one time"""
        toolbox_tool_farm = run_command(["brazil-path", "[BuilderToolboxBundler]pkg.runtimefarm"])
        is_osx = "darwin" in run_command(["uname", "-v"]).lower()
        toolbox_tool_bin = (
            f"{toolbox_tool_farm}/bin/darwin_amd64" if is_osx else f"{toolbox_tool_farm}/bin"
        )
        bundler_cmd = f"{toolbox_tool_bin}/toolbox-bundler"
        publisher_cmd = f"{toolbox_tool_bin}/toolbox-publisher"
        # Bootstrap
        runtime_farm = run_command(["brazil-bootstrap", "--farmType", "copy"])
        package_version = pkg_resources.require("grt-hackathon-team8-mcp")[0].version

        # Clean up
        output_dir = "./build/private/tool-bundle"
        shutil.rmtree(output_dir, ignore_errors=True)

        # Bundle
        bundle_output_dir = run_command(
            [
                bundler_cmd,
                "--root",
                runtime_farm,
                "--os",
                self.toolbox_os,
                # use no version here, since we will determine the actual version in the publisher lambda
                "--tool-version",
                package_version,
                "--metadata",
                "./configuration/toolbox/metadata.json",
                "--output-dir",
                output_dir,
                "--verbose",
            ]
        )

        # Publish
        if self.publish:
            if self.channel == "stable":
                git_status = run_command(["git", "status", "--porcelain"])
                if git_status != "":
                    raise RuntimeError(
                        f"Can't publish to stable channel if there are pending changes: \n{git_status}"
                    )
                git_br_head = run_command(["git", "branch", "-r", "--contains", "HEAD"])
                if git_br_head == "":
                    raise RuntimeError(
                        "Current commit must be merged to remote branch before publishing."
                    )

            # Acquire credentials
            run_command(
                [
                    "ada",
                    "credentials",
                    "update",
                    "--account",
                    self.repository_account,
                    "--role",
                    self.repository_role,
                    "--once",
                ]
            )

            # Publish to toolbox repository
            run_command(
                [
                    publisher_cmd,
                    "--source",
                    f"{bundle_output_dir}/{package_version}",
                    "--publish-to",
                    f"s3://buildertoolbox-{self.repository_name}-us-west-2",
                    "--channel",
                    self.channel,
                    "--make-current",
                    "--verbose",
                ]
            )


# Declare your non-python data files:
# Files underneath configuration/ will be copied into the build preserving the
# subdirectory structure if they exist.
data_files = []
for root, dirs, files in os.walk("configuration"):
    data_files.append(
        (os.path.relpath(root, "configuration"), [os.path.join(root, f) for f in files])
    )

setup(
    # include data files
    data_files=data_files,
    # Add custom commands
    cmdclass={
        "toolbox_bundler": ToolboxBundlerCommand,
    },
)
