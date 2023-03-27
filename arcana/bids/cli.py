import click
from arcana.core.deploy.command import entrypoint_opts
from arcana.bids.deploy import BidsApp
from arcana.core.cli.ext import ext


@ext.group(
    name="bids",
    help="CLI extensions for interacting with BIDS datasets"
)
def bids_group():
    pass


@bids_group.command(
    name="app-entrypoint",
    help="""Loads a dataset, or creates one it is not already present, then applies and
launches a pipeline in a single command. To be used inside BidsApp images.

DATASET_LOCATOR string containing the nickname of the data store, the ID of the
dataset (e.g. XNAT project ID or file-system directory) and the dataset's name
in the format <store-nickname>//<dataset-id>[@<dataset-name>]

""",
)
@click.argument("dataset_locator")
@entrypoint_opts.parameterisation
@entrypoint_opts.execution
@entrypoint_opts.debugging
@entrypoint_opts.dataset_config
def app_entrypoint(
    dataset_locator,
    spec_path,
    **kwargs,
):

    image_spec = BidsApp.load(spec_path)

    image_spec.command.execute(
        dataset_locator,
        **kwargs,
    )
