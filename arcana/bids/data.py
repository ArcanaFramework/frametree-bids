import typing as ty
import json
import re
import logging
import itertools
from operator import itemgetter
import attrs
import jq
from pathlib import Path
from arcana.file_system import DirTree
from fileformats.core import FileSet, Field
from fileformats.medimage.nifti import WithBids
from arcana.core.exceptions import ArcanaUsageError, ArcanaEmptyDatasetError
from arcana.core.data.tree import DataTree
from arcana.core.data.set import Dataset, DatasetMetadata
from arcana.core.data.space import Clinical
from arcana.core.data.entry import DataEntry
from arcana.core.data.row import DataRow

logger = logging.getLogger("arcana")


@attrs.define
class JsonEdit:

    path: str
    # a regular expression matching the paths of files to match (omitting
    # subject/session IDs and extension)
    jq_expr: str
    # a JQ expression (see https://stedolan.github.io/jq/manual/v1.6/) with the
    # exception that '{a_column_name}' will be substituted by the file path of
    # the item matching the column ('{' and '}' need to be escaped by duplicating,
    # i.e. '{{' and '}}').

    @classmethod
    def attr_converter(cls, json_edits: list) -> list:
        if json_edits is None or json_edits is attrs.NOTHING:
            return []
        parsed = []
        for x in json_edits:
            if isinstance(x, JsonEdit):
                parsed.append(x)
            elif isinstance(x, dict):
                parsed.append(JsonEdit(**x))
            else:
                parsed.append(JsonEdit(*x))
        return parsed


@attrs.define(kw_only=True)
class BidsMetadata(DatasetMetadata):

    participants: ty.Dict[str, ty.Dict[str, str]] = attrs.field(
        factory=dict, repr=False
    )
    bids_version: str = attrs.field(default="1.0.1", repr=False)
    bids_type: str = attrs.field(default="derivative", repr=False)


@attrs.define
class Bids(DirTree):
    """Repository for working with data stored on the file-system in BIDS format

    Parameters
    ----------
    json_edits : list[tuple[str, str]], optional
        Specifications to edit JSON files as they are written to the store to
        enable manual modification of fields to correct metadata. List of
        tuples of the form: FILE_PATH - path expression to select the files,
        EDIT_STR - jq filter used to modify the JSON document.
    """

    json_edits: ty.List[JsonEdit] = attrs.field(
        factory=list, converter=JsonEdit.attr_converter
    )

    name: str = "bids"

    PROV_SUFFIX = ".provenance"
    FIELDS_FNAME = "__fields__"
    FILES_PROV_FNAME = "__fields_provenance__"

    #################################
    # Abstract-method implementations
    #################################

    def populate_tree(self, tree: DataTree):
        """
        Find all rows within the dataset stored in the store and
        construct the data tree within the dataset

        Parameters
        ----------
        dataset : Dataset
            The dataset to construct the tree dimensions for
        """

        for subject_id, participant in tree.dataset.participants.items():
            try:
                explicit_ids = {"group": participant["group"]}
            except KeyError:
                explicit_ids = {}
            if tree.dataset.is_multi_session():
                for sess_id in (tree.dataset.root_dir / subject_id).iterdir():
                    tree.add_leaf([subject_id, sess_id.name], explicit_ids=explicit_ids)
            else:
                tree.add_leaf([subject_id], explicit_ids=explicit_ids)

    def populate_row(self, row: DataRow):
        rel_session_path = self.row_path(row)
        root_dir = row.dataset.root_dir
        session_path = root_dir / rel_session_path
        session_path.mkdir(exist_ok=True)
        for modality_dir in session_path.iterdir():
            for entry_fspath in modality_dir.iterdir():
                path = f"{modality_dir.name}/{self._fs2entry_path(entry_fspath)}"
                suffix = "".join(path.suffixes)
                path = path[: -len(suffix)] + "/" + suffix.lstrip(".")
                row.add_entry(
                    path=path,
                    datatype=FileSet,
                    uri=str(entry_fspath.relative_to(root_dir)),
                )
        deriv_dir = root_dir / "derivatives"
        if deriv_dir.exists():
            for pipeline_dir in deriv_dir.iterdir():
                for entry_fspath in (pipeline_dir / rel_session_path).iterdir():
                    if not (
                        entry_fspath.name.startswith(".")
                        or entry_fspath.name
                        not in (self.FIELDS_FNAME, self.FIELDS_PROV_FNAME)
                        or entry_fspath.name.endswith(self.PROV_SUFFIX)
                    ):
                        path = (
                            f"derivatives/{pipeline_dir.name}/"
                            + self._fs2entry_path(entry_fspath.name)
                        )
                        suffix = "".join(path.suffixes)
                        path = path[: -len(suffix)] + "/" + suffix.lstrip(".")
                        row.add_entry(
                            path=path,
                            datatype=FileSet,
                            uri=str(entry_fspath.relative_to(root_dir)),
                        )

    def fileset_uri(self, path: str, datatype: type, row: DataRow) -> str:
        return "derivatives/" + self._entry2fs_path(
            path,
            subject_id=row.ids[Clinical.subject],
            timepoint_id=(
                row.ids[Clinical.timepoint]
                if Clinical.timepoint in row.dataset.hierarchy
                else None
            ),
            ext=datatype.ext,
        )

    def field_uri(self, path: str, datatype: type, row: DataRow) -> str:
        try:
            pipeline_name, field_name = path.split("/")
        except ValueError:
            raise ArcanaUsageError(
                f"Field path '{path}', should contain two sections delimted by '/', "
                "the first is the pipeline name that generated the field, "
                "and the second the field name"
            )
        return (
            "derivatives/"
            + self._entry2fs_path(
                f"{pipeline_name}/{self.FIELDS_FNAME}",
                subject_id=row.ids[Clinical.subject],
                timepoint_id=(
                    row.ids[Clinical.timepoint]
                    if Clinical.timepoint in row.dataset.hierarchy
                    else None
                ),
            )
        ) + f"@{field_name}"

    def get_fileset(self, entry: DataEntry, datatype: type) -> FileSet:
        return datatype(self._fileset_fspath(entry))

    def put_fileset(self, fileset: FileSet, entry: DataEntry) -> FileSet:
        """
        Inserts or updates a fileset in the store
        """
        fspath = self._fileset_fspath(entry)
        # Create target directory if it doesn't exist already
        copied_fileset = fileset.copy_to(
            dest_dir=fspath.parent, stem=fspath.name, make_dirs=True
        )
        if isinstance(fileset, WithBids):
            # Ensure TaskName field is present in the JSON side-car if task
            # is in the filename
            self._edit_nifti_x(fileset)
        return copied_fileset

    def get_field(self, entry: DataEntry, datatype: type) -> Field:
        fspath, key = self._fields_fspath_and_key(entry)
        return datatype(self.read_from_json(fspath, key))

    def put_field(self, field: Field, entry: DataEntry):
        """
        Inserts or updates a field in the store
        """
        fspath, key = self._fields_fspath_and_key(entry)
        self.update_json(fspath, key, field.raw_type(field))

    def get_fileset_provenance(self, entry: DataEntry) -> dict[str, ty.Any]:
        with open(self._fileset_prov_fspath(entry)) as f:
            provenance = json.load(f)
        return provenance

    def put_fileset_provenance(self, provenance: dict[str, ty.Any], entry: DataEntry):
        with open(self._fileset_prov_fspath(entry), "w") as f:
            json.dump(provenance, f)

    def get_field_provenance(self, entry: DataEntry) -> dict[str, ty.Any]:
        fspath, key = self._fields_prov_fspath_and_key(entry)
        with open(fspath) as f:
            fields_provenance = json.load(f)
        return fields_provenance[key]

    def put_field_provenance(self, provenance: dict[str, ty.Any], entry: DataEntry):
        fspath, key = self._fields_prov_fspath_and_key(entry)
        self.update_json(fspath, key, provenance)

    # Override method in base to use sub-classed metadata
    def define_dataset(self, *args, metadata=None, **kwargs):
        return super().define_dataset(*args, metadata=self._convert_metadata(metadata), **kwargs)

    def _convert_metadata(self, metadata):
        if metadata is None:
            metadata = {}
        elif isinstance(metadata, DatasetMetadata):
            metadata = attrs.asdict(metadata)
        metadata = BidsMetadata(**metadata)
        return metadata

    ###############
    # Other methods
    ###############

    def create_dataset(
        self,
        id: str,
        subject_ids: list[str],
        timepoint_ids: list[str] = None,
        name: str = None,
        **kwargs,
    ):
        if timepoint_ids is not None:
            hierarchy = ["subject", "timepoint"]
        else:
            hierarchy = ["session"]
        # if readme is None:
        #     readme = "Mock readme\n" * 20
        # if authors is None:
        #     authors = ["Mock A. Author", "Mock B. Author"]
        root_dir = Path(id)
        root_dir.mkdir(parents=True)
        participants = {}
        # Create rows
        if timepoint_ids:
            session_iter = itertools.product(subject_ids, timepoint_ids)
        else:
            session_iter = zip(subject_ids, itertools.repeat(None))
        for subject_id, timepoint_id in session_iter:
            if not subject_id.startswith("sub-"):
                subject_id = f"sub-{subject_id}"
            participants[subject_id] = {}
            if timepoint_id and not timepoint_id.startswith("ses-"):
                timepoint_id = f"ses-{timepoint_id}"
            sess_dir_fspath = root_dir / self._entry2fs_path(
                entry_path=None, subject_id=subject_id, timepoint_id=timepoint_id
            )
            sess_dir_fspath.mkdir(parents=True)
        dataset = self.define_dataset(
            id=id,
            space=Clinical,
            hierarchy=hierarchy,
            name=name,
            **kwargs,
        )
        dataset.save()
        return dataset

    def save_dataset(self, dataset: Dataset, name: str = None, overwrite: bool = False):

        root_dir = Path(dataset.id)

        participants_fspath = root_dir / "participants.tsv"
        if participants_fspath.exists() and not overwrite:
            logger.warning(
                "Not attempting to overwrite existing BIDS dataset description at "
                f"'{str(participants_fspath)}"
            )
        else:
            if not dataset.metadata.participants:
                raise ArcanaUsageError(
                    "A BIDS dataset needs at least one participant before the metadata "
                    "can be saved"
                )

            with open(participants_fspath, "w") as f:
                col_names = list(next(iter(self.participants.values())).keys())
                f.write("\t".join(["participant_id"] + col_names) + "\n")
                for pcpt_id, pcpt_attrs in self.participants.items():
                    f.write(
                        "\t".join([pcpt_id] + [pcpt_attrs[c] for c in col_names]) + "\n"
                    )

        dataset_description_fspath = root_dir / "dataset_description.json"
        if dataset_description_fspath.exists() and not overwrite:
            logger.warning(
                "Not attempting to overwrite existing BIDS dataset description at "
                f"'{str(dataset_description_fspath)}"
            )
        else:
            metadata_dict = attrs.asdict(dataset.metadata, recurse=True)
            dataset_description = {k: metadata_dict[a] for a, k in METADATA_MAPPING}

            with open(dataset_description_fspath, "w") as f:
                json.dump(dataset_description, f, indent="    ")

        if dataset.metadata.readme is not None:
            readme_path = root_dir / "README"
            if readme_path.exists() and not overwrite:
                logger.warning(
                    "Not attempting to overwrite existing BIDS dataset description at "
                    f"'{str(dataset_description_fspath)}"
                )
            else:
                with open(readme_path, "w") as f:
                    f.write(self.readme)
        super().save_dataset(dataset, name=name)

    def load_dataset(self, id, name=None):
        from arcana.core.data.set import (
            Dataset,
        )  # avoid circular imports it is imported here rather than at the top of the file

        

    ################
    # Helper methods
    ################

    def _fileset_fspath(self, entry):
        return Path(entry.row.dataset.id) / entry.uri

    def _fields_fspath_and_key(self, entry):
        relpath, key = entry.uri.split("@")
        fspath = Path(entry.row.dataset.id) / relpath
        return fspath, key

    def _fileset_prov_fspath(self, entry):
        return self._fileset_fspath(entry).with_suffix(self.PROV_SUFFIX)

    def _fields_prov_fspath_and_key(self, entry):
        fields_fspath, key = self._fields_fspath_and_key(entry)
        return fields_fspath.parent / self.FIELDS_PROV_FNAME, key

    def _edit_nifti_x(self, fileset: WithBids):
        """Edit JSON files as they are written to manually modify the JSON
        generated by the dcm2niix where required

        Parameters
        ----------
        fspath : str
            Path of the JSON to potentially edit
        """
        dct = None

        def lazy_load_json():
            if dct is not None:
                return dct
            else:
                with open(fileset.side_car) as f:
                    return json.load(f)

        # Ensure there is a value for TaskName for files that include 'task-taskname'
        # in their file path
        if match := re.match(r".*task-([a-zA-Z]+).*", fileset.path):
            dct = lazy_load_json()
            if "TaskName" not in dct:
                dct["TaskName"] = match.group(1)
        # Get dictionary containing file paths for all items in the same row
        # as the file-set so they can be used in the edits using Python
        # string templating
        col_paths = {}
        for col_name, item in fileset.row.items():
            rel_path = self.fileset_stem_path(item).relative_to(
                fileset.row.dataset.root_dir / self.row_path(fileset.row)
            )
            col_paths[col_name] = str(rel_path) + fileset.ext

        for jedit in self.json_edits:
            jq_expr = jedit.jq_expr.format(**col_paths)  # subst col file paths
            if re.match(jedit.path, fileset.path):
                dct = jq.compile(jq_expr).input(lazy_load_json()).first()
        # Write dictionary back to file if it has been loaded
        if dct is not None:
            with open(fileset.side_car, "w") as f:
                json.dump(dct, f)

    @classmethod
    def _extract_entities(cls, relpath):
        path = relpath.parent
        stem = relpath.name.split(".")[0]
        parts = stem.split("_")
        path /= parts[-1]
        entities = sorted((tuple(p.split("-")) for p in parts[:-1]), key=itemgetter(0))
        return str(path), entities

    @classmethod
    def _fs2entry_path(cls, relpath: Path) -> str:
        """Converts a BIDS filename into an Arcana "entry-path".
        Entities not corresponding to subject and session IDs

        Parameters
        ----------
        relpath : Path
            the relative path to the file from the subject/session directory

        Returns
        -------
        entry_path : str
            the "path" of an entry relative to the subject/session row.
        """
        entry_path, entities = cls._extract_entities(relpath)
        for key, val in entities:
            if key not in ("sub", "ses"):
                entry_path += f"/{key}={val}"
        return entry_path

    @classmethod
    def _entry2fs_path(
        cls, entry_path: str, subject_id: str, timepoint_id: str = None, ext: str = ""
    ) -> Path:
        """Converts a BIDS filename into an Arcana "entry-path".
        Entities not corresponding to subject and session IDs

        Parameters
        ----------
        path : str
            a path of an entry to be converted into a BIDS file-path
        subject_id : str
            the subject ID of the entry
        timepoint_id : str, optional
            the session ID of the entry, by default None
        ext : str, optional
            file extension to be appended to the path, by default ""

        Returns
        -------
        rel_path : Path
            relative path to the file corresponding to the given entry path
        """
        if entry_path is not None:
            parts = entry_path.rstrip("/").split("/")
            if len(parts) < 2:
                raise ArcanaUsageError(
                    "BIDS paths should contain at least two '/' delimited parts (e.g. "
                    f"anat/T1w or freesurfer/recon-all), given '{entry_path}'"
                )
        fname = f"sub-{subject_id}_"
        relpath = Path(f"sub-{subject_id}")
        if timepoint_id is not None:
            fname += f"ses-{timepoint_id}_"
            relpath /= f"ses-{timepoint_id}"
        if entry_path is not None:
            entities = []
            for part in parts[2:]:
                if "=" in part:
                    entities.append(part.split("="))
                else:
                    relpath /= part
            fname += (
                "_".join("-".join(e) for e in sorted(entities, key=itemgetter(0)))
                + "_"
                + parts[1]
            )
            relpath /= fname
            if ext:
                relpath = relpath.with_suffix(ext)
        return relpath


def outputs_converter(outputs):
    """Sets the path of an output to '' if not provided or None"""
    return [o[:2] + ("",) if len(o) < 3 or o[2] is None else o for o in outputs]


METADATA_MAPPING = (
    ("name", "Name"),
    ("bids_version", "BIDSVersion"),
    ("bids_type", "DatasetType"),
    ("license", "Licence"),
    ("authors", "Authors"),
    ("acknowledgements", "Acknowledgements"),
    ("how_to_acknowledge", "HowToAcknowledge"),
    ("funding", "Funding"),
    ("ethics_approvals", "EthicsApprovals"),
    ("references", "ReferencesAndLinks"),
    ("doi", "DatasetDOI"),
    ("generated_by", "GeneratedBy"),
    ("sources", "SourceDatasets"),
)
