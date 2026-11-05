import typing as ty
import json
import itertools
from pathlib import Path
from warnings import warn
import requests.exceptions
import nibabel as nb
import numpy.random
import shutil
from dataclasses import dataclass
import pytest
import docker.errors
from fileformats.application import Json
from fileformats.generic import Directory
from fileformats.medimage import NiftiX
from fileformats.text import Plain as Text
from frametree.core import __version__
from frametree.axes.medimage import MedImage
from frametree.bids.store import Bids


MOCK_BIDS_APP_NAME = "mockapp"
MOCK_README = "A dummy readme\n" * 100
MOCK_AUTHORS = ["Dumm Y. Author", "Another D. Author"]


def test_bids_roundtrip(bids_validator_docker, bids_success_str, work_dir):

    path = work_dir / "bids-dataset"
    dataset_name = "adataset"

    shutil.rmtree(path, ignore_errors=True)
    dataset = Bids().create_dataset(
        id=path,
        name=dataset_name,
        axes=MedImage,
        hierarchy=["group", "subject", "visit"],
        leaves=[
            (group, f"{group}{member}", visit)
            for group, member, visit in itertools.product(
                ["test", "control"],
                [str(i) for i in range(1, 4)],
                [str(i) for i in range(1, 3)],
            )
        ],
        id_patterns={
            "member": r"subject::\w+(\d+)",
        },
        metadata={
            "description": MOCK_README,
            "authors": MOCK_AUTHORS,
            "generated_by": [
                {
                    "name": "frametree",
                    "version": __version__,
                    "description": "FrameSet was created programmatically from scratch",
                    "code_url": "http://frametree.readthedocs.io",
                }
            ],
        },
    )

    dataset.add_sink("t1w", datatype=NiftiX, path="anat/T1w")

    dummy_nifti = work_dir / "t1w.nii"
    # dummy_nifti_gz = dummy_nifti + '.gz'
    dummy_json = work_dir / "t1w.json"

    # Create a random Nifti file to satisfy BIDS parsers
    hdr = nb.Nifti1Header()
    hdr.set_data_shape((10, 10, 10))
    hdr.set_zooms((1.0, 1.0, 1.0))  # set voxel size
    hdr.set_xyzt_units(2)  # millimeters
    hdr.set_qform(numpy.diag([1, 2, 3, 1]))
    nb.save(
        nb.Nifti1Image(
            numpy.random.randint(0, 1, size=[10, 10, 10]),
            hdr.get_best_affine(),
            header=hdr,
        ),
        dummy_nifti,
    )

    with open(dummy_json, "w") as f:
        json.dump({"test": "json-file", "SkullStripped": False}, f)

    for row in dataset.rows(frequency="session"):
        row["t1w"] = (dummy_nifti, dummy_json)

    # Full dataset validation using dockerized validator
    dc = docker.from_env()
    try:
        dc.images.pull(bids_validator_docker)
    except requests.exceptions.HTTPError:
        warn("No internet connection, so couldn't download latest BIDS validator")
    container = dc.containers.create(
        bids_validator_docker,
        command="/data",
        volumes=[f"{path}:/data:ro"],
        detach=False,
    )
    try:
        container.start()
        result = container.wait()
        logs = container.logs(stdout=True, stderr=True)
    finally:
        container.remove(force=True)
    assert (
        result["StatusCode"] == 0
    ), f"BIDS validator failed with exit code {result['StatusCode']}, logs:\n{logs.decode()}"

    reloaded = Bids().load_frameset(id=path, name=dataset_name)
    reloaded.add_sink(
        "t1w", datatype=NiftiX, path="anat/T1w"
    )  # add sink to reloaded so it matches
    reloaded.name = ""  # remove saved name so it matches

    assert dataset == reloaded


@dataclass
class SourceNiftiXBlueprint:
    """The blueprint for the source nifti files"""

    path: str  # BIDS path for Nift
    orig_side_car: dict
    edited_side_car: dict


@dataclass
class JsonEditBlueprint:

    source_niftis: ty.Dict[str, SourceNiftiXBlueprint]
    path_re: str  # regular expression for the paths to edit
    jq_script: str  # jq script


JSON_EDIT_TESTS = {
    "basic": JsonEditBlueprint(
        path_re="anat/T.*w",
        jq_script=".a.b += 4",
        source_niftis={
            "t1w": SourceNiftiXBlueprint(
                path="anat/T1w",
                orig_side_car={"a": {"b": 1.0}},
                edited_side_car={"a": {"b": 5.0}},
            )
        },
    ),
    "multiple": JsonEditBlueprint(
        path_re="anat/T.*w",
        jq_script=".a.b += 4 | .a.c[] *= 2",
        source_niftis={
            "t1w": SourceNiftiXBlueprint(
                path="anat/T1w",
                orig_side_car={"a": {"b": 1.0, "c": [2, 4, 6]}},
                edited_side_car={"a": {"b": 5.0, "c": [4, 8, 12]}},
            )
        },
    ),
    "fmap": JsonEditBlueprint(
        path_re="fmap/.*",
        jq_script='.IntendedFor = "{bold}"',
        source_niftis={
            "bold": SourceNiftiXBlueprint(
                path="func/bold/task=rest",
                orig_side_car={},
                edited_side_car={"TaskName": "rest"},
            ),
            "fmap_mag1": SourceNiftiXBlueprint(
                path="fmap/magnitude1",
                orig_side_car={},
                edited_side_car={"IntendedFor": "func/sub-1_task-rest_bold.nii"},
            ),
            "fmap_mag2": SourceNiftiXBlueprint(
                path="fmap/magnitude2",
                orig_side_car={},
                edited_side_car={"IntendedFor": "func/sub-1_task-rest_bold.nii"},
            ),
            "fmap_phasediff": SourceNiftiXBlueprint(
                path="fmap/phasediff",
                orig_side_car={},
                edited_side_car={"IntendedFor": "func/sub-1_task-rest_bold.nii"},
            ),
        },
    ),
}


@pytest.fixture(params=JSON_EDIT_TESTS)
def json_edit_blueprint(request):
    return JSON_EDIT_TESTS[request.param]


def test_bids_json_edit(json_edit_blueprint: JsonEditBlueprint, work_dir: Path):

    bp = json_edit_blueprint  # shorten name

    path = work_dir / "bids-dataset"
    name = "bids-dataset"

    shutil.rmtree(path, ignore_errors=True)
    dataset = Bids(
        json_edits=[(bp.path_re, bp.jq_script)],
    ).create_dataset(
        id=path,
        name=name,
        leaves=[("1",)],
        metadata={
            "description": MOCK_README,
            "authors": MOCK_AUTHORS,
            "generated_by": [
                {
                    "name": "frametree",
                    "version": __version__,
                    "description": "FrameSet was created programmatically from scratch",
                    "code_url": "http://frametree.readthedocs.io",
                }
            ],
        },
    )

    for sf_name, sf_bp in bp.source_niftis.items():
        dataset.add_sink(sf_name, datatype=NiftiX, path=sf_bp.path)

        nifti_fspath = work_dir / (sf_name + ".nii")
        # dummy_nifti_gz = dummy_nifti + '.gz'
        json_fspath = work_dir / (sf_name + ".json")

        # Create a random Nifti file to satisfy BIDS parsers
        hdr = nb.Nifti1Header()
        hdr.set_data_shape((10, 10, 10))
        hdr.set_zooms((1.0, 1.0, 1.0))  # set voxel size
        hdr.set_xyzt_units(2)  # millimeters
        hdr.set_qform(numpy.diag([1, 2, 3, 1]))
        nb.save(
            nb.Nifti1Image(
                numpy.random.randint(0, 1, size=[10, 10, 10]),
                hdr.get_best_affine(),
                header=hdr,
            ),
            nifti_fspath,
        )

        with open(json_fspath, "w") as f:
            json.dump(sf_bp.orig_side_car, f)

        # Get single item in dataset
        dataset[sf_name]["1"] = (nifti_fspath, json_fspath)

    # Check edited JSON matches reference
    for sf_name, sf_bp in bp.source_niftis.items():

        item = dataset[sf_name]["1"]
        with open(item.json_file) as f:
            saved_dict = json.load(f)

        assert saved_dict == sf_bp.edited_side_car


@pytest.fixture
def derivatives_dataset(work_dir: Path):
    """A BIDS dataset with a derivative laid out like the output of a BIDS app"""
    dataset = Bids().create_dataset(
        id=work_dir / "bids-dataset",
        name="adataset",
        leaves=[("1",)],
        metadata={"authors": MOCK_AUTHORS},
    )
    subject_dir = Path(dataset.id) / "derivatives" / "app" / "sub-1"
    (subject_dir / "anat").mkdir(parents=True)
    for fname in (
        "sub-1_file1.txt",
        "anat/sub-1_T2w.txt",
        "anat/sub-1_desc-brain_mask.txt",
    ):
        (subject_dir / fname).write_text(fname)
    return dataset


@pytest.mark.parametrize(
    "path,datatype,expected",
    [
        ("file1", Text, "sub-1_file1.txt"),
        ("anat", Directory, "anat"),
        ("anat/T2w", Text, "anat/sub-1_T2w.txt"),
        ("anat/mask/desc=brain", Text, "anat/sub-1_desc-brain_mask.txt"),
    ],
)
def test_bids_derivative_paths(
    derivatives_dataset, path: str, datatype: type, expected: str
):
    derivatives_dataset.add_sink("output", datatype, path=f"{path}@app")
    row = derivatives_dataset.row(MedImage.session, "1")
    with derivatives_dataset.store.connection:
        item = row["output"]
    subject_dir = Path(derivatives_dataset.id) / "derivatives" / "app" / "sub-1"
    assert Path(str(item)) == subject_dir / expected


def test_bids_derivative_directory_with_nested_directory(derivatives_dataset):
    """A nested directory doesn't make its parent directory's path ambiguous"""
    subject_dir = Path(derivatives_dataset.id) / "derivatives" / "app" / "sub-1"
    (subject_dir / "log" / "20260101-000000_abcd").mkdir(parents=True)
    derivatives_dataset.add_sink("output", Directory, path="log@app")
    row = derivatives_dataset.row(MedImage.session, "1")
    with derivatives_dataset.store.connection:
        item = row["output"]
    assert Path(str(item)) == subject_dir / "log"


def test_bids_derivative_non_bids_nested_file(derivatives_dataset):
    """Files in sub-directories that don't follow BIDS naming are skipped, not fatal"""
    subject_dir = Path(derivatives_dataset.id) / "derivatives" / "app" / "sub-1"
    (subject_dir / "anat" / "not_a-valid-entity_name.txt").write_text("")
    derivatives_dataset.add_sink("output", Text, path="anat/T2w@app")
    row = derivatives_dataset.row(MedImage.session, "1")
    with derivatives_dataset.store.connection:
        item = row["output"]
    assert Path(str(item)) == subject_dir / "anat" / "sub-1_T2w.txt"


def test_bids_derivative_nested_roundtrip(work_dir: Path):
    """A derivative written to a sub-directory can be read back from a fresh load"""
    path = work_dir / "bids-dataset"
    dataset = Bids().create_dataset(
        id=path, name="adataset", leaves=[("1",)], metadata={"authors": MOCK_AUTHORS}
    )
    dataset.add_sink("output", Json, path="anat/T2w@app")
    source = work_dir / "source.json"
    source.write_text('{"a": 1}')
    dataset["output"]["1"] = source

    reloaded = Bids().load_frameset(id=path, name="adataset")
    reloaded.add_sink("output", Json, path="anat/T2w@app")
    row = reloaded.row(MedImage.session, "1")
    with reloaded.store.connection:
        item = row["output"]
    assert Path(str(item)).parts[-3:] == ("sub-1", "anat", "sub-1_T2w.json")
    assert Path(str(item)).read_text() == '{"a": 1}'
