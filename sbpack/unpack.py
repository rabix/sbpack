# Copyright (c) 2020 Seven Bridges
# Given a cwl dict, if it is a workflow, split out any inlined steps into their own files

from typing import Tuple, List
import pathlib
import sys

import ruamel.yaml

import sevenbridges.errors as sbgerr

from .lib import get_profile
from .version import __version__
from cwlformat.formatter import stringify_dict


import logging

logger = logging.getLogger(__name__)

yaml = ruamel.yaml.YAML()


class CWLProcess:
    def __init__(self, cwl: dict, file_path: pathlib.Path):
        self.cwl = cwl
        self.file_path = file_path

    def __str__(self):
        return stringify_dict(self.cwl)

    def save(self):
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.file_path.write_text(stringify_dict(self.cwl))


def explode(cwl: CWLProcess) -> List[CWLProcess]:
    _processes = [cwl]
    _cwl = cwl.cwl
    if _cwl.get("class") == "Workflow":
        sanitize_id(_cwl)
        _cwl_steps = _cwl.get("steps", {})
        _is_dict = isinstance(_cwl_steps, dict)
        for _k, _step in (_cwl_steps.items() if _is_dict else enumerate(_cwl_steps)):
            _step_id = _k if _is_dict else _step.get("id")
            if _step_id is not None:
                _run = _step.get("run")
                if isinstance(_run, dict):
                    step_path = \
                        cwl.file_path.parent / \
                        (cwl.file_path.name + ".steps") / \
                        (_step_id + ".cwl")
                    _step["run"] = str(step_path.relative_to(cwl.file_path.parent))
                    _processes += explode(CWLProcess(_run, step_path))

    return _processes


def sanitize_id(cwl: dict):
    # cwltool bug: https://github.com/common-workflow-language/cwltool/issues/1280
    if "id" in cwl:
        cwl["sbg:original_source"] = cwl["id"]
        cwl.pop("id")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=f"sbpull v{__version__}\n"
                    "Download and, optionally, explode CWL workflow\n"
                    "from any Seven Bridges powered platform\n"
                    "(c) Seven Bridges 2020\n")
    parser.add_argument("profile")
    parser.add_argument("appid")
    parser.add_argument("outname")
    parser.add_argument("--unpack", action='store_true')
    args = parser.parse_args()

    api = get_profile(args.profile)

    try:
        app = api.apps.get(args.appid)
        as_dict = app.raw
    except sbgerr.NotFound:
        sys.stderr.write(f"{args.appid} not found.\n")
        return 1

    fp_out = pathlib.Path(args.outname).absolute()
    if not args.unpack:
        sys.stderr.write(f"Saving {args.appid} to {fp_out}\n")
        fp_out.write_text(stringify_dict(as_dict))
        return 0

    for n, exploded in enumerate(explode(CWLProcess(as_dict, fp_out))):
        sys.stderr.write(f"{n + 1}: {exploded.file_path.relative_to(fp_out.parent)}\n")
        exploded.save()


if __name__ == "__main__":
    main()
