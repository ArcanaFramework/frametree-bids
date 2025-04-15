# from functools import reduce
# from operator import mul
# import pytest
# from fileformats.text import Plain as Text
# from frametree.testing.blueprint import (
#     TestDatasetBlueprint,
#     FileSetEntryBlueprint as FileBP,
# )
# from frametree.common import Clinical
# from fileformats.medimage import NiftiGzX
# from frametree.bids.cli import app_entrypoint
# from frametree.core.serialize import ClassResolver
# from frametree.core.utils import path2varname
# from frametree.core.utils import show_cli_trace
# from pydra2app.core.image import App
# from frametree.bids.store import Bids


# @pytest.mark.xfail(reason="Still implementing BIDS app entrypoint")
# def test_bids_app_entrypoint(
#     mock_bids_app_executable, cli_runner, nifti_sample_dir, work_dir
# ):

#     blueprint = TestDatasetBlueprint(
#         axes=Clinical,
#         hierarchy=["subject", "session"],
#         dim_lengths=[1, 1, 1],
#         entries=[
#             FileBP(
#                 path="anat/T1w",
#                 datatype=NiftiGzX,
#                 filenames=["anat/T1w.nii.gz", "anat/T1w.json"],
#             ),
#             FileBP(
#                 path="anat/T2w",
#                 datatype=NiftiGzX,
#                 filenames=["anat/T2w.nii.gz", "anat/T2w.json"],
#             ),
#             FileBP(
#                 "dwi/dwi",
#                 datatype=NiftiGzX,
#                 filenames=[
#                     "dwi/dwi.nii.gz",
#                     "dwi/dwi.json",
#                     "dwi/dwi.bvec",
#                     "dwi/dwi.bval",
#                 ],
#             ),
#         ],
#         derivatives=[
#             FileBP(
#                 path="file1",
#                 row_frequency=Clinical.session,
#                 datatype=Text,
#                 filenames=["file1.txt"],
#             ),
#             FileBP(
#                 path="file2",
#                 row_frequency=Clinical.session,
#                 datatype=Text,
#                 filenames=["file2.txt"],
#             ),
#         ],
#     )

#     dataset_path = work_dir / "bids-dataset"

#     dataset = blueprint.make_dataset(
#         dataset_id=dataset_path, store=Bids(), source_data=nifti_sample_dir
#     )

#     spec_path = work_dir / "spec.yaml"

#     blueprint = dataset.__annotations__["blueprint"]

#     address = f"{dataset_path}"
#     # Start generating the arguments for the CLI
#     # Add source to loaded dataset
#     args = [
#         address,
#         "--plugin",
#         "debug",
#         "--work",
#         str(work_dir),
#         "--spec-path",
#         spec_path,
#         "--dataset-hierarchy",
#         ",".join(blueprint.hierarchy),
#     ]
#     inputs_config = {}
#     for path, (datatype, _) in blueprint.expected_datatypes.items():
#         format_str = ClassResolver.tostr(datatype)
#         varname = path2varname(path)
#         inputs_config[varname] = {
#             "configuration": {
#                 "path": path,
#             },
#             "datatype": format_str,
#             "help": "dummy",
#         }

#     outputs_config = {}
#     for path, _, datatype, _ in blueprint.derivatives:
#         format_str = ClassResolver.tostr(datatype)
#         varname = path2varname(path)
#         outputs_config[varname] = {
#             "configuration": {
#                 "path": path,
#             },
#             "datatype": format_str,
#             "help": "dummy",
#         }

#     image_spec = App(
#         title="a test image",
#         name="test_bids_app_entrypoint",
#         version={"package": "1.0", "build": "1"},
#         authors=[{"name": "Some One", "email": "some.one@an.email.org"}],
#         docs={
#             "info_url": "http://concatenate.readthefakedocs.io",
#         },
#         command={
#             "task": "frametree.bids.tasks:bids_app",
#             "row_frequency": "medimage:Clinical[session]",
#             "inputs": inputs_config,
#             "outputs": outputs_config,
#             "configuration": {
#                 "executable": str(mock_bids_app_executable),
#             },
#         },
#         packages={"pip": ["frametree-bids"]},
#     )
#     image_spec.save(spec_path)

#     result = cli_runner(app_entrypoint, args)
#     assert result.exit_code == 0, show_cli_trace(result)
#     # Add source column to saved dataset
#     for fname in ["file1", "file2"]:
#         sink = dataset.add_sink(fname, Text)
#         assert len(sink) == reduce(mul, blueprint.dim_lengths)
#         for item in sink:
#             item.get(assume_exists=True)
#             with open(item.fspath) as f:
#                 contents = f.read()
#             assert contents == fname + "\n"
