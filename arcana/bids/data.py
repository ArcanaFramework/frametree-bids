import typing as ty
import json
import re
import logging
import itertools
from operator import itemgetter
from collections import defaultdict
import attrs
import jq
from pathlib import Path
from arcana.core.data.store import LocalStore
from fileformats.core import FileSet, Field
from fileformats.medimage.nifti import WithBids
from arcana.core.exceptions import ArcanaUsageError
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

    PROV_SUFFIX = ".provenance"
    FIELDS_FNAME = "__fields__"
    FILES_PROV_FNAME = "__fields_provenance__"

    VERSION_KEY = "BidsVersion"
    VERSION = "1.0.1"

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

    def scan_row(self, row: DataRow):
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
        row_ids: list[list[str]],
        space: type = Clinical,
        name: str = None,
        metadata: dict[str, ty.Any] = None,
        **kwargs,
    ):
        root_dir = Path(id)
        root_dir.mkdir(parents=True)
        if metadata is None:
            metadata = {}
        if "participants" not in metadata:
            metadata["participants"] = defaultdict(dict)
        # Create rows
        for ids_tuple in itertools.product(*row_ids.values()):
            ids = dict(zip(row_ids, ids_tuple))
            subject_id = ids["subject"]
            timepoint_id = ids.get("timepoint")
            group_id = ids.get("group")
            if "timepoint" in row_ids:
                subject_id, timepoint_id = ids_tuple
                if timepoint_id.startswith("ses-"):
                    timepoint_id = f"ses-{timepoint_id}"
            else:
                subject_id = ids_tuple[0]
                timepoint_id = None
            if not subject_id.startswith("sub-"):
                subject_id = f"sub-{subject_id}"
            metadata["participants"]["participant_id"] = subject_id
            if group_id is not None:
                metadata["participants"][subject_id]["group"] = group_id
            sess_dir_fspath = root_dir / self._entry2fs_path(
                entry_path=None, subject_id=subject_id, timepoint_id=timepoint_id
            )
            sess_dir_fspath.mkdir(parents=True)
        dataset = self.define_dataset(
            id=id,
            space=space,
            hierarchy=list(row_ids),
            name=name,
            metadata=metadata,
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
                    rw = [subject_row.id] + [
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
                    f.write(self.readme)

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
        m[1]: dct[m[0]] if len(m) == 2 else map_to_bids_names(dict[m[0]], mappings=m[2])
        for m in mappings
        if dct[m[0]] is not None
    }


def map_from_bids_names(dct, mappings=METADATA_MAPPING):
    return {
        m[0]: dct[m[1]] if len(m) == 2 else map_to_bids_names(dict[m[1]], mappings=m[2])
        for m in mappings
        if dct[m[1]] is not None
    }
