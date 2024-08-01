import os
import stat
from pathlib import Path
import shutil
import pytest
from fileformats.medimage import NiftiGzX, NiftiGzXBvec
from frametree.bids.tasks import bids_app, BidsInput, BidsOutput
from fileformats.text import Plain as Text
from fileformats.generic import Directory


MOCK_BIDS_APP_NAME = "mockapp"
MOCK_README = "A dummy readme\n" * 100
MOCK_AUTHORS = ["Dumm Y. Author", "Another D. Author"]

BIDS_INPUTS = [
    BidsInput(name="T1w", path="anat/T1w", datatype=NiftiGzX),
    BidsInput(name="T2w", path="anat/T2w", datatype=NiftiGzX),
    BidsInput(name="dwi", path="dwi/dwi", datatype=NiftiGzXBvec),
]
BIDS_OUTPUTS = [
    BidsOutput(name="whole_dir", datatype=Directory),  # whole derivative directory
    BidsOutput(name="a_file", path="file1", datatype=Text),
    BidsOutput(name="another_file", path="file2", datatype=Text),
]


@pytest.mark.xfail(reason="Need to convert this to new environment syntax")
def test_bids_app_docker(
    bids_validator_app_image: str, nifti_sample_dir: Path, work_dir: Path
):

    kwargs = {}

    bids_dir = work_dir / "bids"

    shutil.rmtree(bids_dir, ignore_errors=True)

    task = bids_app(
        name=MOCK_BIDS_APP_NAME,
        container_image=bids_validator_app_image,
        executable=None,  # uses entrypoint
        inputs=BIDS_INPUTS,
        outputs=BIDS_OUTPUTS,
        dataset=bids_dir,
    )

    for inpt in BIDS_INPUTS:
        kwargs[inpt.name] = nifti_sample_dir.joinpath(
            *inpt.path.split("/")
        ).with_suffix(inpt.datatype.ext)

    result = task(plugin="serial", **kwargs)

    for output in BIDS_OUTPUTS:
        assert Path(getattr(result.output, output.name)).exists()


def test_bids_app_naked(
    mock_bids_app_script: str, nifti_sample_dir: Path, work_dir: Path
):

    # Create executable that runs validator then produces some mock output
    # files
    launch_sh = work_dir / "launch.sh"
    # We don't need to run the full validation in this case as it is already tested by test_run_bids_app_docker
    # so we use the simpler test script.
    with open(launch_sh, "w") as f:
        f.write(mock_bids_app_script)

    os.chmod(launch_sh, stat.S_IRWXU)

    task = bids_app(
        name=MOCK_BIDS_APP_NAME,
        executable=launch_sh,  # Extracted using `docker_image_executable(docker_image)`
        inputs=BIDS_INPUTS,
        outputs=BIDS_OUTPUTS,
        app_output_dir=work_dir / "output",
    )

    kwargs = {}
    for inpt in BIDS_INPUTS:
        kwargs[inpt.name] = nifti_sample_dir.joinpath(
            *inpt.path.split("/")
        ).with_suffix(inpt.datatype.ext)

    bids_dir = work_dir / "bids"

    shutil.rmtree(bids_dir, ignore_errors=True)

    result = task(plugin="serial", **kwargs)

    for output in BIDS_OUTPUTS:
        assert Path(getattr(result.output, output.name)).exists()
