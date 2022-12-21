from __future__ import annotations
from pathlib import Path
import shlex
import attrs
from neurodocker.reproenv import DockerRenderer
from arcana.core.deploy.image import App


@attrs.define(kw_only=True)
class BidsApp(App):

    def add_entrypoint(self, dockerfile: DockerRenderer, build_dir: Path):

        command_line = (
            self.command.activate_conda_cmd() + "arcana ext bids app-entrypoint"
        )

        dockerfile.entrypoint(shlex.split(command_line))
