import typing as ty
import json
import re
import logging
import attrs
from dataclasses import dataclass
import jq
from pathlib import Path
from arcana.file_system import DirTree
from fileformats.core import FileSet, Field
from fileformats.serialization import Json
from arcana.core.exceptions import ArcanaUsageError, ArcanaEmptyDatasetError
from arcana.core.data.set import DataTree
from arcana.core.data.entry import DataEntry
from arcana.core.data.row import DataRow

logger = logging.getLogger("arcana")


@dataclass
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

    def populate_tree(self, tree: DataTree):
        """
        Find all rows within the dataset stored in the store and
        construct the data tree within the dataset

        Parameters
        ----------
        dataset : Dataset
            The dataset to construct the tree dimensions for
        """

        try:
            tree.dataset.load_metadata()
        except ArcanaEmptyDatasetError:
            return

        for subject_id, participant in tree.dataset.participants.items():
            try:
                explicit_ids = {"group": participant["group"]}
            except KeyError:
                explicit_ids = {}
            if tree.dataset.is_multi_session():
                for sess_id in (tree.dataset.root_dir / subject_id).iterdir():
                    tree.add_leaf([subject_id, sess_id], explicit_ids=explicit_ids)
            else:
                tree.add_leaf([subject_id], explicit_ids=explicit_ids)

    def populate_row(self, row: DataRow):
        rel_session_path = self.row_path(row)
        root_dir = row.dataset.root_dir
        session_path = root_dir / rel_session_path
        session_path.mkdir(exist_ok=True)
        for modality_dir in session_path.iterdir():
            self.add_entries_from_dir(modality_dir, row)
        deriv_dir = root_dir / "derivatives"
        if deriv_dir.exists():
            for pipeline_dir in deriv_dir.iterdir():
                self.add_entries_from_dir(pipeline_dir / rel_session_path, row)

    def get_fileset_path(self, entry: DataEntry) -> Path:
        row = entry.row
        fspath = self.root_dir(row)
        parts = entry.id.split("/")
        if parts[-1] == "":
            parts = parts[:-1]
        if parts[0] == "derivatives":
            if len(parts) < 2:
                raise ArcanaUsageError(
                    "Paths should have another part after 'derivatives'"
                )
            elif len(parts) == 2 and not entry.datatype.is_dir:
                raise ArcanaUsageError(
                    "Single-level derivative paths must be of type directory "
                    f"({entry.id}: {entry.datatype.mime_like})"
                )
            # append the first to parts of the path before the row ID (e.g. sub-01/ses-02)
            fspath = fspath.joinpath(*parts[:2])
            parts = parts[2:]
        fspath /= self.row_path(row)
        if parts:  # The whole derivatives directories can be the output for a BIDS app
            for part in parts[:-1]:
                fspath /= part
            fname = (
                "_".join(row.ids[h] for h in row.dataset.hierarchy) + "_" + parts[-1]
            )
            fspath /= fname
        return fspath

    def get_all_fileset_paths(self, fspath: Path):
        fspaths = set(super().get_all_fileset_paths(fspath))
        # Get inherited side-cars
        for fspath in fspaths:
            entities = self.get_file_entities(fspath)
            parent = fspath.parent
            for parent in parent:
                for inherited in parent.iterdir():
                    if inherited.is_file():
                        inherited_entities = self.get_file_entities(inherited)
                        if (inherited_entities & entities) == entities:
                            fspaths.add(inherited)
        return fspaths

    def get_fields_path(self, entry: DataEntry) -> Path:
        parts = entry.id.split("/")
        if parts[0] != "derivatives":
            assert False, "Non-derivative fields should be taken from participants.tsv"
        return (
            entry.row.dataset.root_dir.joinpath(parts[:2])
            / self.row_path(entry.row)
            / self.FIELDS_FNAME
        )

    def get_field(self, entry: DataEntry) -> Field:
        row = entry.row
        dataset = row.dataset
        if entry.id in dataset.participant_attrs:
            val = entry.datatype(dataset.participants[row.ids["subject"]])
        else:
            val = super().get_field(entry)
        return val

    def put_fileset(self, fileset: FileSet, entry: DataEntry) -> FileSet:
        stored_fileset = super().put_fileset(fileset, entry)
        if hasattr(stored_fileset, "side_car") and isinstance(stored_fileset.side_car, Json):
            # Ensure TaskName field is present in the JSON side-car if task
            # is in the filename
            self._edit_side_car(fileset)
        return stored_fileset

    def _edit_side_car(self, fileset: FileSet):
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
            col_paths[col_name] = str(rel_path) + "." + fileset.ext

        for jedit in self.json_edits:
            jq_expr = jedit.jq_expr.format(**col_paths)  # subst col file paths
            if re.match(jedit.path, fileset.path):
                dct = jq.compile(jq_expr).input(lazy_load_json()).first()
        # Write dictionary back to file if it has been loaded
        if dct is not None:
            with open(fileset.side_car, "w") as f:
                json.dump(dct, f)

        @classmethod
        def get_file_entities(cls, fspath):
            stem = fspath.name.split(".")[0]
            return set(tuple(e.split("-")) for e in stem.split('_')[1:])


def outputs_converter(outputs):
    """Sets the path of an output to '' if not provided or None"""
    return [o[:2] + ("",) if len(o) < 3 or o[2] is None else o for o in outputs]
