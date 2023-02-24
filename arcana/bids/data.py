import typing as ty
import json
import re
import logging
import itertools
from operator import itemgetter
import attrs
import jq
from pathlib import Path
from arcana.core.data.store import LocalStore
from fileformats.core import FileSet, Field
from fileformats.generic import Directory
from fileformats.medimage.nifti import WithBids
from arcana.core.exceptions import ArcanaUsageError
from arcana.core.data.tree import DataTree
from arcana.core.data.set import Dataset
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

    PROV_SUFFIX = ".provenance"
    FIELDS_FNAME = "__fields__"
    FIELDS_PROV_FNAME = "__fields_provenance__"

    #################################
    # Abstract-method implementations
    #################################

    def scan_tree(self, tree: DataTree):
        """
        Find all rows within the dataset stored in the store and
        construct the data tree within the dataset

        Parameters
        ----------
        dataset : Dataset
            The dataset to construct the tree dimensions for
        """
        root_dir = Path(tree.dataset.id)
        participants_fspath = root_dir / "participants.tsv"
        participants = {}
        with open(participants_fspath) as f:
            lines = f.read().splitlines()
        if lines:
            participant_keys = lines[0].split("\t")
            for line in lines[1:]:
                dct = dict(zip(participant_keys, line.split("\t")))
                participants[dct.pop("participant_id")[len("sub-") :]] = dct

        for subject_dir in root_dir.iterdir():
            if not subject_dir.name.startswith("sub-"):
                continue
            subject_id = subject_dir.name[len("sub-") :]
            try:
                additional_ids = {"group": participants[subject_id]["group"]}
            except KeyError:
                additional_ids = {}
            if any(d.name.startswith("ses-") for d in subject_dir.iterdir()):
                for sess_dir in subject_dir.iterdir():
                    sess_id = sess_dir.name[len("ses-") :]
                    tree.add_leaf([subject_id, sess_id], additional_ids=additional_ids)
            else:
                tree.add_leaf([subject_id], additional_ids=additional_ids)

    def scan_row(self, row: DataRow):
        root_dir = row.dataset.root_dir
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
                                "@"
                                + pipeline_dir.name
                                + "/"
                                + self._fs2entry_path(entry_fspath.name)
                            )
                            # suffix = "".join(entry_fspath.suffixes)
                            # path = path[: -len(suffix)] + "/" + suffix.lstrip(".")
                            row.add_entry(
                                path=path,
                                datatype=FileSet,
                                uri=str(entry_fspath.relative_to(root_dir)),
                            )

    def fileset_uri(self, path: str, datatype: type, row: DataRow) -> str:
        if path.startswith("@"):  # derivative
            base_uri = "derivatives/"
            path = path[1:]
        else:
            base_uri = ""
        return base_uri + str(
            self._entry2fs_path(
                path,
                subject_id=row.ids[Clinical.subject],
                timepoint_id=(
                    row.ids[Clinical.timepoint]
                    if Clinical.timepoint in row.dataset.hierarchy
                    else None
                ),
                ext=datatype.ext,
            )
        )

    def field_uri(self, path: str, datatype: type, row: DataRow) -> str:
        if path.startswith("@"):  # derivative
            base_uri = "derivatives/"
            path = path[1:]
        else:
            base_uri = ""
        try:
            namespace, field_name = path.split("/")
        except ValueError:
            raise ArcanaUsageError(
                f"Field path '{path}', should contain two sections delimted by '/', "
                "the first is the pipeline name that generated the field, "
                "and the second the field name"
            )
        return (
            base_uri
            + self._entry2fs_path(
                f"{namespace}/{self.FIELDS_FNAME}",
                subject_id=row.ids[Clinical.subject],
                timepoint_id=(
                    row.ids[Clinical.timepoint]
                    if Clinical.timepoint in row.dataset.hierarchy
                    else None
                ),
            )
        ) + f"::{field_name}"

    def get_fileset(self, entry: DataEntry, datatype: type) -> FileSet:
        return datatype(self._fileset_fspath(entry))

    def put_fileset(self, fileset: FileSet, entry: DataEntry) -> FileSet:
        """
        Inserts or updates a fileset in the store
        """
        fspath = self._fileset_fspath(entry)
        # Create target directory if it doesn't exist already
        copied_fileset = fileset.copy_to(
            dest_dir=fspath.parent,
            stem=fspath.name[: -len(fileset.ext)],
            make_dirs=True,
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
    # def define_dataset(self, *args, metadata=None, **kwargs):
    #     return super().define_dataset(*args, metadata=self._convert_metadata(metadata), **kwargs)

    # def _convert_metadata(self, metadata):
    #     if metadata is None:
    #         metadata = {}
    #     elif isinstance(metadata, DatasetMetadata):
    #         metadata = attrs.asdict(metadata)
    #     metadata = BidsMetadata(**metadata)
    #     return metadata

    ###############
    # Other methods
    ###############

    def create_empty_dataset(
        self,
        id: str,
        row_ids: dict[str, list[str]],
        space: type = Clinical,
        name: str = None,
        **kwargs,
    ):
        root_dir = Path(id)
        root_dir.mkdir(parents=True)
        group_ids = {}
        # Create sub-directories corresponding to rows of the dataset
        for ids_tuple in itertools.product(*row_ids.values()):
            ids = dict(zip(row_ids, ids_tuple))
            subject_id = ids["subject"]
            timepoint_id = ids.get("timepoint")
            group_id = ids.get("group")
            if group_id is not None:
                subject_id = group_id + str(subject_id)
                group_ids[subject_id] = group_id
            sess_dir_fspath = root_dir / self._entry2fs_path(
                entry_path=None, subject_id=subject_id, timepoint_id=timepoint_id
            )
            sess_dir_fspath.mkdir(parents=True)
        # Save group IDs in participants TSV
        if group_ids:
            with open(root_dir / "participants.tsv", "w") as f:
                f.write("participant_id\tgroup\n")
                for subject_id, group_id in group_ids.items():
                    f.write(f"sub-{subject_id}\t{group_id}\n")
            participants_desc = {
                "group": {
                    "Description": "the group the participant belonged to",
                    "Levels": {g: f"{g} group" for g in row_ids["group"]},
                }
            }
            with open(root_dir / "participants.json", "w") as f:
                json.dump(participants_desc, f)
        dataset = self.define_dataset(
            id=id,
            space=space,
            hierarchy=(
                ["subject", "timepoint"] if "timepoint" in row_ids else ["session"]
            ),
            name=name,
            **kwargs,
        )
        dataset.save()
        return dataset

    def save_dataset(
        self, dataset: Dataset, name: str = None, overwrite_metadata: bool = False
    ):

        super().save_dataset(dataset, name=name)
        root_dir = Path(dataset.id)
        participants_fspath = root_dir / "participants.tsv"
        if participants_fspath.exists() and not overwrite_metadata:
            logger.warning(
                "Not attempting to overwrite existing BIDS dataset description at "
                f"'{str(participants_fspath)}"
            )
        else:
            with open(participants_fspath, "w") as f:
                col_names = ["participant_id"] + dataset.metadata.row_keys
                if len(dataset.row_ids(Clinical.group)) > 1:
                    col_names.append("group")
                f.write("\t".join(col_names) + "\n")
                for subject_row in dataset.rows(frequency=Clinical.subject):
                    rw = ["sub-" + subject_row.id] + [
                        subject_row.metadata[k] for k in dataset.metadata.row_keys
                    ]
                    if "group" in col_names:
                        rw.append(subject_row.ids[Clinical.group])
                    f.write("\t".join(rw) + "\n")

        dataset_description_fspath = root_dir / "dataset_description.json"
        if dataset_description_fspath.exists() and not overwrite_metadata:
            logger.warning(
                "Not attempting to overwrite existing BIDS dataset description at "
                f"'{str(dataset_description_fspath)}"
            )
        else:
            dataset_description = map_to_bids_names(
                attrs.asdict(dataset.metadata, recurse=True)
            )
            dataset_description["BIDSVersion"] = self.BIDS_VERSION
            with open(dataset_description_fspath, "w") as f:
                json.dump(dataset_description, f, indent="    ")

        if dataset.metadata.readme is not None:
            readme_path = root_dir / "README"
            if readme_path.exists() and not overwrite_metadata:
                logger.warning(
                    "Not attempting to overwrite existing BIDS dataset description at "
                    f"'{str(dataset_description_fspath)}"
                )
            else:
                with open(readme_path, "w") as f:
                    f.write(dataset.metadata.readme)

    # def load_dataset(self, id, name=None):
    #     from arcana.core.data.set import (
    #         Dataset,
    #     )  # avoid circular imports it is imported here rather than at the top of the file

    ################
    # Helper methods
    ################

    def _fileset_fspath(self, entry):
        return Path(entry.row.dataset.id) / entry.uri

    def _fields_fspath_and_key(self, entry):
        relpath, key = entry.uri.split("::")
        fspath = Path(entry.row.dataset.id) / relpath
        return fspath, key

    def _fileset_prov_fspath(self, entry):
        return self._fileset_fspath(entry).with_suffix(self.PROV_SUFFIX)

    def _fields_prov_fspath_and_key(self, entry):
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
            if cell.is_empty:
                cell_uri = self.fileset_uri(cell.column.path, cell.datatype, entry.row)
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
    def _extract_entities(cls, relpath):
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
        entry_path, entities, suffix = cls._extract_entities(relpath)
        for key, val in entities:
            if key not in ("sub", "ses"):
                entry_path += f"/{key}={val}"
        return entry_path + "/" + suffix

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
        fname = f"sub-{subject_id}"
        relpath = Path(f"sub-{subject_id}")
        if timepoint_id is not None:
            fname += f"_ses-{timepoint_id}"
            relpath /= f"ses-{timepoint_id}"
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
    def _rel_row_path(cls, row: DataRow):
        relpath = Path(f"sub-{row.ids[Clinical.subject]}")
        if Clinical.timepoint in row.dataset.hierarchy:
            relpath /= f"ses-{row.ids[Clinical.timepoint]}"
        return relpath

    def definition_save_path(self, dataset_id, name):
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
        m[1]: dct[m[0]]
        if len(m) == 2
        else [map_to_bids_names(i, mappings=m[2]) for i in dct[m[0]]]
        for m in mappings
        if dct[m[0]] is not None
    }


def map_from_bids_names(dct, mappings=METADATA_MAPPING):
    return {
        m[0]: dct[m[1]]
        if len(m) == 2
        else [map_to_bids_names(i, mappings=m[2]) for i in dict[m[1]]]
        for m in mappings
        if dct[m[1]] is not None
    }
