from __future__ import annotations
import typing as ty
import json
import re
import logging
from operator import itemgetter
import attrs
import jq
from pathlib import Path
from frametree.core.store import LocalStore
from fileformats.core import FileSet, Field
from fileformats.generic import Directory
from fileformats.medimage.nifti import WithBids, NiftiGzX
from pydra.utils.typing import is_fileset_or_union
from frametree.core.exceptions import FrameTreeUsageError
from frametree.core.tree import DataTree
from frametree.core.frameset import FrameSet
from frametree.common import Clinical
from frametree.core.entry import DataEntry
from frametree.core.row import DataRow

logger = logging.getLogger("frametree")


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


@attrs.define
class Bids(LocalStore):
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

    BIDS_VERSION = "1.0.1"
    DEFAULT_AXES = Clinical

    PROV_SUFFIX = ".provenance"
    FIELDS_FNAME = "__fields__"
    FIELDS_PROV_FNAME = "__fields_provenance__"

    VALID_HIERARCHIES = (
        ["subject", "visit"],
        ["session"],
        ["group", "subject", "visit"],
        ["group", "session"],
    )

    DEFAULT_DATATYPE = NiftiGzX

    #################################
    # Abstract-method implementations
    #################################

    def populate_tree(self, tree: DataTree):
        """
        Find all rows within the dataset stored in the store and
        construct the data tree within the dataset

        Parameters
        ----------
        dataset : FrameSet
            The dataset to construct the tree dimensions for
        """
        root_dir = Path(tree.frameset.id)
        if "group" in tree.frameset.hierarchy:
            with open(root_dir / "participants.tsv") as f:
                lines = f.read().splitlines()
            participants = {}
            if lines:
                participant_keys = lines[0].split("\t")
                for line in lines[1:]:
                    dct = dict(zip(participant_keys, line.split("\t")))
                    participants[dct.pop("participant_id")[len("sub-") :]] = dct
        for subject_dir in root_dir.iterdir():
            if not subject_dir.name.startswith("sub-"):
                continue
            subject_id = subject_dir.name[len("sub-") :]
            if "group" in tree.frameset.hierarchy:
                tree_path = [participants[subject_id]["group"]]
            else:
                tree_path = []
            tree_path.append(subject_id)
            if any(d.name.startswith("ses-") for d in subject_dir.iterdir()):
                for sess_dir in subject_dir.iterdir():
                    visit_id = sess_dir.name[len("ses-") :]
                    tree.add_leaf(tree_path + [visit_id])
            else:
                tree.add_leaf([subject_id])

    def populate_row(self, row: DataRow):
        root_dir = row.frameset.root_dir
        relpath = self._rel_row_path(row)
        session_path = root_dir / relpath
        session_path.mkdir(exist_ok=True)
        for modality_dir in session_path.iterdir():
            for entry_fspath in modality_dir.iterdir():
                # suffix = "".join(entry_fspath.suffixes)
                path = self._fs2entry_path(entry_fspath.relative_to(session_path))
                # path = path.split(".")[0] + "/" + suffix.lstrip(".")
                row.add_entry(
                    path=path,
                    datatype=FileSet,
                    uri=str(entry_fspath.relative_to(root_dir)),
                )
        deriv_dir = root_dir / "derivatives"
        if deriv_dir.exists():
            for pipeline_dir in deriv_dir.iterdir():
                pipeline_row_dir = pipeline_dir / relpath
                if pipeline_row_dir.exists():
                    # Add in the whole row directory as an entry
                    row.add_entry(
                        path="@" + pipeline_dir.name,
                        datatype=Directory,
                        uri=pipeline_row_dir.relative_to(root_dir),
                    )
                    for entry_fspath in pipeline_row_dir.iterdir():
                        if not (
                            entry_fspath.name.startswith(".")
                            or entry_fspath.name
                            in (self.FIELDS_FNAME, self.FIELDS_PROV_FNAME)
                            or entry_fspath.name.endswith(self.PROV_SUFFIX)
                        ):
                            path = (
                                self._fs2entry_path(entry_fspath.name)
                                + "@"
                                + pipeline_dir.name
                            )
                            # suffix = "".join(entry_fspath.suffixes)
                            # path = path[: -len(suffix)] + "/" + suffix.lstrip(".")
                            row.add_entry(
                                path=path,
                                datatype=FileSet,
                                uri=str(entry_fspath.relative_to(root_dir)),
                            )

    def fileset_uri(self, path: str, datatype: type, row: DataRow) -> str:
        path, dataset_name = DataEntry.split_dataset_name_from_path(path)
        if dataset_name is None:
            base_uri = ""
        elif not dataset_name:
            base_uri = f"derivatives/{self.EMPTY_DATASET_NAME}"
        else:
            base_uri = f"derivatives/{dataset_name}"
        return base_uri + str(
            self._entry2fs_path(
                path,
                subject_id=row.frequency_id("subject"),
                visit_id=(
                    row.frequency_id("visit")
                    if "visit" in row.frameset.hierarchy
                    else None
                ),
                ext=datatype.ext,
            )
        )

    def field_uri(self, path: str, datatype: type, row: DataRow) -> str:
        path, dataset_name = DataEntry.split_dataset_name_from_path(path)
        if dataset_name is None:
            base_uri = ""
        elif not dataset_name:
            base_uri = f"derivatives/{self.EMPTY_DATASET_NAME}"
        else:
            base_uri = f"derivatives/{dataset_name}"
        try:
            namespace, field_name = path.split("/")
        except ValueError:
            raise FrameTreeUsageError(
                f"Field path '{path}', should contain two sections delimited by '/', "
                "the first is the pipeline name that generated the field, "
                "and the second the field name"
            )
        return (
            str(
                Path(base_uri)
                / self._entry2fs_path(
                    f"{namespace}/{self.FIELDS_FNAME}",
                    subject_id=row.frequency_id("subject"),
                    visit_id=(
                        row.frequency_id("visit")
                        if Clinical.visit in row.frameset.hierarchy
                        else None
                    ),
                )
            )
            + f"::{field_name}"
        )

    def get_fileset(self, entry: DataEntry, datatype: type) -> FileSet:
        return datatype(self._fileset_fspath(entry))

    def put_fileset(self, fileset: FileSet, entry: DataEntry) -> FileSet:
        """
        Inserts or updates a fileset in the store
        """
        fspath = self._fileset_fspath(entry)
        # Create target directory if it doesn't exist already
        copied_fileset = fileset.copy(
            dest_dir=fspath.parent,
            new_stem=fspath.name[: -len(fileset.ext)],
            make_dirs=True,
            overwrite=entry.is_derivative,
        )
        if isinstance(copied_fileset, WithBids):
            # Ensure TaskName field is present in the JSON side-car if task
            # is in the filename
            self._edit_nifti_x(copied_fileset, entry)
        return copied_fileset

    def get_field(self, entry: DataEntry, datatype: type) -> Field:
        fspath, key = self._fields_fspath_and_key(entry)
        return datatype(self.read_from_json(fspath, key))

    def put_field(self, field: Field, entry: DataEntry):
        """
        Inserts or updates a field in the store
        """
        fspath, key = self._fields_fspath_and_key(entry)
        self.update_json(fspath, key, field.primitive(field))

    def get_fileset_provenance(self, entry: DataEntry) -> ty.Dict[str, ty.Any]:
        with open(self._fileset_prov_fspath(entry)) as f:
            provenance = json.load(f)
        return provenance

    def put_fileset_provenance(
        self, provenance: ty.Dict[str, ty.Any], entry: DataEntry
    ):
        with open(self._fileset_prov_fspath(entry), "w") as f:
            json.dump(provenance, f)

    def get_field_provenance(self, entry: DataEntry) -> ty.Dict[str, ty.Any]:
        fspath, key = self._fields_prov_fspath_and_key(entry)
        with open(fspath) as f:
            fields_provenance = json.load(f)
        return fields_provenance[key]

    def put_field_provenance(self, provenance: ty.Dict[str, ty.Any], entry: DataEntry):
        fspath, key = self._fields_prov_fspath_and_key(entry)
        self.update_json(fspath, key, provenance)

    def create_data_tree(
        self,
        id: str,
        leaves: ty.List[ty.Tuple[str, ...]],
        hierarchy: ty.List[str],
        **kwargs,
    ):
        if hierarchy not in self.VALID_HIERARCHIES:
            raise FrameTreeUsageError(
                f"Invalid hierarchy {hierarchy} provided to create a new data tree "
                f"needs to be one of the following:\n"
                + "\n".join(str(h) for h in self.VALID_HIERARCHIES)
            )
        root_dir = Path(id)
        root_dir.mkdir(parents=True)
        # Create sub-directories corresponding to rows of the dataset
        group_ids = set()
        subjects_group_id = {}
        for ids_tuple in leaves:
            ids = dict(zip(hierarchy, ids_tuple))
            # Add in composed IDs
            try:
                subject_id = ids["subject"]
            except KeyError:
                subject_id = ids["session"]
            visit_id = ids.get("visit")
            group_id = ids.get("group")
            if group_id:
                group_ids.add(group_id)
                subjects_group_id[subject_id] = group_id
            sess_dir_fspath = root_dir / self._entry2fs_path(
                entry_path=None, subject_id=subject_id, visit_id=visit_id
            )
            sess_dir_fspath.mkdir(parents=True, exist_ok=True)
        # Add participants.tsv to define the groups if present
        if group_ids:
            with open(root_dir / "participants.tsv", "w") as f:
                f.write("participant_id\tgroup\n")
                for subject_id, group_id in subjects_group_id.items():
                    f.write(f"sub-{subject_id}\t{group_id}\n")

    ####################
    # Overrides of API #
    ####################

    def save_frameset(self, dataset: FrameSet, name: ty.Optional[str] = None):
        super().save_frameset(dataset, name=name)
        self._save_metadata(dataset)

    def create_dataset(
        self,
        id: str,
        leaves: ty.List[ty.Tuple[str, ...]],
        hierarchy: ty.List[str] = ["session"],
        axes: type = Clinical,
        name: ty.Optional[str] = None,
        **kwargs,
    ):
        """Creates a new dataset with new rows to store data in

        Parameters
        ----------
        id : str
            ID of the dataset
        leaves : list[tuple[str, ...]]
            the list of tuple IDs (at each level of the tree)
        name : str, optional
            name of the dataset, if provided the dataset definition will be saved. To
            save the dataset with the default name pass an empty string.
        hierarchy : list[str], optional
            hierarchy of the dataset tree, by default single level (i.e. one session per subject)
        axes : type, optional
            the axes of the dataset

        Returns
        -------
        FrameSet
            the newly created dataset
        """
        dataset = super().create_dataset(
            id=id, leaves=leaves, hierarchy=hierarchy, axes=axes, name=name, **kwargs
        )
        self._save_metadata(dataset)
        return dataset

    ################
    # Helper methods
    ################

    def _save_metadata(self, dataset: FrameSet):
        root_dir = Path(dataset.id)
        dataset_description_fspath = root_dir / "dataset_description.json"
        dataset_description = map_to_bids_names(
            attrs.asdict(dataset.metadata, recurse=True)
        )
        dataset_description["BIDSVersion"] = self.BIDS_VERSION
        with open(dataset_description_fspath, "w") as f:
            json.dump(dataset_description, f, indent="    ")

        if dataset.metadata.description is not None:
            readme_path = root_dir / "README"
            with open(readme_path, "w") as f:
                f.write(dataset.metadata.description)
        columns = list(dataset.metadata.row_metadata)
        group_ids = [i for i in dataset.row_ids("group") if i is not None]
        if group_ids or columns:
            subject_rows = dataset.rows("subject")
            with open(dataset.root_dir / "participants.tsv", "w") as f:
                f.write("participant_id")
                if group_ids:
                    f.write("\tgroup")
                if columns:
                    f.write("\t" + "\t".join(columns))
                f.write("\n")
                for row in subject_rows:
                    f.write(f"sub-{row.id}")
                    if group_ids:
                        f.write("\t" + row.frequency_id("group"))
                    if columns:
                        f.write("\t" + "\t".join(row.metadata[k] for k in columns))
                    f.write("\n")
            participants_desc = {}
            if group_ids:
                participants_desc["group"] = {
                    "Description": "the group the participant belonged to",
                    "Levels": {g: f"{g} group" for g in dataset.row_ids("group")},
                }
            for name, desc in dataset.metadata.row_metadata.items():
                participants_desc[name] = {"Description": desc}
            with open(dataset.root_dir / "participants.json", "w") as f:
                json.dump(participants_desc, f)

    def _fileset_fspath(self, entry: DataEntry) -> Path:
        return Path(entry.row.frameset.id) / entry.uri

    def _fields_fspath_and_key(self, entry: DataEntry) -> ty.Tuple[Path, str]:
        relpath, key = entry.uri.split("::")
        fspath = Path(entry.row.frameset.id) / relpath
        return fspath, key

    def _fileset_prov_fspath(self, entry: DataEntry) -> Path:
        return self._fileset_fspath(entry).with_suffix(self.PROV_SUFFIX)

    def _fields_prov_fspath_and_key(self, entry: DataEntry) -> ty.Tuple[Path, str]:
        fields_fspath, key = self._fields_fspath_and_key(entry)
        return fields_fspath.parent / self.FIELDS_PROV_FNAME, key

    def _edit_nifti_x(self, nifti_x: WithBids, entry: DataEntry):
        """Edit JSON files as they are written to manually modify the JSON
        generated by the dcm2niix where required

        Parameters
        ----------
        fspath : str
            Path of the JSON to potentially edit
        """
        with open(nifti_x.json_file) as f:
            json_dict = json.load(f)

        # Ensure there is a value for TaskName for files that include 'task-taskname'
        # in their file path
        if match := re.match(r".*/task=([^/]+)", entry.path):
            if "TaskName" not in json_dict:
                json_dict["TaskName"] = match.group(1)
        # Get dictionary containing file paths for all items in the same row
        # as the file-set so they can be used in the edits using Python
        # string templating
        col_fspaths = {}
        for cell in entry.row.cells():
            if is_fileset_or_union(cell.datatype):
                if cell.is_empty:
                    cell_uri = self.fileset_uri(
                        cell.column.path, cell.datatype, entry.row
                    )
                else:
                    cell_uri = cell.entry.uri
                try:
                    col_fspaths[cell.column.name] = Path(cell_uri).relative_to(
                        self._rel_row_path(entry.row)
                    )
                except ValueError:
                    pass
        for jedit in self.json_edits:
            jq_expr = jedit.jq_expr.format(**col_fspaths)  # subst col file paths
            if re.match(jedit.path, entry.path):
                json_dict = jq.compile(jq_expr).input(json_dict).first()
        # Write dictionary back to file if it has been loaded
        with open(nifti_x.json_file, "w") as f:
            json.dump(json_dict, f)

    @classmethod
    def _extract_entities(
        cls, relpath: Path
    ) -> ty.Tuple[str, ty.List[ty.Tuple[str, ...]], str]:
        relpath = Path(relpath)
        path = relpath.parent
        name_parts = relpath.name.split(".")
        stem = name_parts[0]
        suffix = ".".join(name_parts[1:])
        parts = stem.split("_")
        path /= parts[-1]
        entities = sorted((tuple(p.split("-")) for p in parts[:-1]), key=itemgetter(0))
        return str(path), entities, suffix

    @classmethod
    def _fs2entry_path(cls, relpath: Path) -> str:
        """Converts a BIDS filename into an FrameTree "entry-path".
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
        entry_path, entities, suffix = cls._extract_entities(relpath)
        for entity in entities:
            try:
                key, val = entity
            except ValueError as e:
                raise FrameTreeUsageError(
                    f"Invalid entity {entity!r} in path '{relpath}'"
                ) from e
            if key not in ("sub", "ses"):
                entry_path += f"/{key}={val}"
        return entry_path + "/" + suffix

    @classmethod
    def _entry2fs_path(
        cls,
        entry_path: str,
        subject_id: str,
        visit_id: ty.Optional[str] = None,
        ext: str = "",
    ) -> Path:
        """Converts a BIDS filename into an FrameTree "entry-path".
        Entities not corresponding to subject and session IDs

        Parameters
        ----------
        path : str
            a path of an entry to be converted into a BIDS file-path
        subject_id : str
            the subject ID of the entry
        visit_id : str, optional
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
                raise FrameTreeUsageError(
                    "BIDS paths should contain at least two '/' delimited parts (e.g. "
                    f"anat/T1w or freesurfer/recon-all), given '{entry_path}'"
                )
        fname = f"sub-{subject_id}"
        relpath = Path(f"sub-{subject_id}")
        if visit_id is not None:
            fname += f"_ses-{visit_id}"
            relpath /= f"ses-{visit_id}"
        if entry_path is not None:
            entities = []
            relpath /= parts[0]  # BIDS data type or dataset/pipeline name
            for part in parts[2:]:
                if "=" in part:
                    entities.append(part.split("="))
                else:
                    relpath /= part
            fname += (
                "".join(
                    f"_{k}-{v}" for k, v in sorted(entities, key=itemgetter(0))
                )  # BIDS entities
                + "_"
                + parts[1]  # BIDS modality suffix
            )
            relpath /= fname
            if ext:
                relpath = relpath.with_suffix(ext)
        return relpath

    @classmethod
    def _rel_row_path(cls, row: DataRow) -> Path:
        relpath = Path(f"sub-{row.frequency_id('subject')}")
        if "visit" in row.frameset.hierarchy:
            relpath /= f"ses-{row.frequency_id('visit')}"
        return relpath

    def definition_save_path(self, dataset_id: str, name: str) -> Path:
        return Path(dataset_id) / "derivatives" / name / "definition.yaml"


def outputs_converter(outputs):
    """Sets the path of an output to '' if not provided or None"""
    return [o[:2] + ("",) if len(o) < 3 or o[2] is None else o for o in outputs]


METADATA_MAPPING = (
    ("name", "Name"),
    ("type", "DatasetType"),
    ("license", "Licence"),
    ("authors", "Authors"),
    ("acknowledgements", "Acknowledgements"),
    ("how_to_acknowledge", "HowToAcknowledge"),
    ("funding", "Funding"),
    ("ethics_approvals", "EthicsApprovals"),
    ("references", "ReferencesAndLinks"),
    ("doi", "DatasetDOI"),
    (
        "generated_by",
        "GeneratedBy",
        (
            ("name", "Name"),
            ("description", "Description"),
            ("code_url", "CodeURL"),
            (
                "container",
                "Container",
                (
                    ("type", "Type"),
                    ("tag", "Tag"),
                    ("uri", "URI"),
                ),
            ),
        ),
    ),
    (
        "sources",
        "SourceDatasets",
        (
            ("url", "URL"),
            ("doi", "DOI"),
            ("version", "Version"),
        ),
    ),
)


def map_to_bids_names(dct, mappings=METADATA_MAPPING):
    return {
        m[1]: (
            dct[m[0]]
            if len(m) == 2
            else [map_to_bids_names(i, mappings=m[2]) for i in dct[m[0]]]
        )
        for m in mappings
        if dct[m[0]] is not None
    }


def map_from_bids_names(dct, mappings=METADATA_MAPPING):
    return {
        m[0]: (
            dct[m[1]]
            if len(m) == 2
            else [map_to_bids_names(i, mappings=m[2]) for i in ty.Dict[m[1]]]
        )
        for m in mappings
        if dct[m[1]] is not None
    }
