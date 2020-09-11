"""
The link resolution is as follows:

We always have two components: the base and the link
If the link is a url or absolute path it is what is used to fetch the data.
If the link is a relative path it is combined with the base and that is what is
used to fetch data
"""

#  Copyright (c) 2020 Seven Bridges. See LICENSE

import sys
import pathlib
import urllib.parse
import urllib.request
from typing import Union
from copy import deepcopy
import json
import enum

from ruamel.yaml import YAML

import sevenbridges as sbg
import sevenbridges.errors as sbgerr

import sbpack.schemadef as schemadef
import sbpack.lib as lib

from .version import __version__

import logging

logger = logging.getLogger(__name__)

fast_yaml = YAML(typ="safe")


def get_inner_dict(cwl: dict, path: list):
    if len(path) == 0:
        return cwl

    if isinstance(cwl, dict):
        _v = cwl.get(path[0]["key"])
        if _v is not None:
            return get_inner_dict(_v, path[1:])

    elif isinstance(cwl, list):  # Going to assume this is a map expressed as list
        for _v in cwl:
            if isinstance(_v, dict):
                if _v.get(path[0]["key_field"]) == path[0]["key"]:
                    return get_inner_dict(_v, path[1:])

    return None


def pack_process(cwl: dict, base_url: urllib.parse.ParseResult):
    cwl = listify_everything(cwl)
    # cwl = dictify_requirements(cwl)
    cwl = normalize_sources(cwl)
    cwl = resolve_schemadefs(cwl, base_url)
    cwl = resolve_imports(cwl, base_url)
    cwl = resolve_linked_processes(cwl, base_url)
    cwl = add_missing_requirements(cwl)
    return cwl


def listify_everything(cwl: dict):
    for port in ["inputs", "outputs"]:
        cwl[port] = lib.normalize_to_list(
            cwl.get(port, []), key_field="id", value_field="type"
        )

    cwl["requirements"] = lib.normalize_to_list(
        cwl.get("requirements", []), key_field="class", value_field=None
    )

    if cwl.get("class") != "Workflow":
        return cwl

    cwl["steps"] = lib.normalize_to_list(
        cwl.get("steps", []), key_field="id", value_field=None
    )

    for n, v in enumerate(cwl["steps"]):
        if isinstance(v, dict):
            v["in"] = lib.normalize_to_list(
                v.get("in", []), key_field="id", value_field="source"
            )

    return cwl


def dictify_requirements(cwl: dict):
    cwl["requirements"] = lib.normalize_to_map(
        cwl.get("requirements", {}), key_field="class"
    )
    return cwl


def normalize_sources(cwl: dict):
    if cwl.get("class") != "Workflow":
        return cwl

    for _step in cwl.get("steps"):
        if not isinstance(_step, dict):
            continue

        _inputs = _step.get("in")
        for k, _input in enumerate(_inputs):
            if isinstance(_input, str):
                _inputs[k] = _normalize(_input)
            elif isinstance(_input, dict):
                _src = _input.get("source")
                if isinstance(_src, str):
                    _input["source"] = _normalize(_input["source"])

    _outputs = cwl.get("outputs")
    for k, _output in enumerate(_outputs):
        if isinstance(_output, str):
            _outputs[k] = _normalize(_output)
        elif isinstance(_output, dict):
            _src = _output.get("outputSource")
            if isinstance(_src, str):
                _output["outputSource"] = _normalize(_output["outputSource"])

    return cwl


def _normalize(s):
    if s.startswith("#"):
        return s[1:]
    else:
        return s


def resolve_schemadefs(cwl: dict, base_url: urllib.parse.ParseResult):
    user_defined_types = schemadef.build_user_defined_type_dict(cwl, base_url)
    cwl["requirements"] = [
        req
        for req in cwl.get("requirements", [])
        if req.get("class") != "SchemaDefRequirement"
    ]
    cwl = schemadef.inline_types(cwl, "inputs", base_url, user_defined_types)
    cwl = schemadef.inline_types(cwl, "outputs", base_url, user_defined_types)
    return cwl


def resolve_imports(cwl: dict, base_url: urllib.parse.ParseResult):
    if isinstance(cwl, dict):
        itr = cwl.items()
    elif isinstance(cwl, list):
        itr = [(n, v) for n, v in enumerate(cwl)]
    else:
        return cwl

    for k, v in itr:
        if isinstance(v, dict):
            if len(v) == 1:
                _k = list(v.keys())[0]
                if _k in ["$import", "$include"]:
                    cwl[k], this_base_url = lib.load_linked_file(
                        base_url, v[_k], is_import=_k == "$import"
                    )

        cwl[k] = resolve_imports(cwl[k], base_url)

    return cwl


def resolve_linked_processes(cwl: dict, base_url: urllib.parse.ParseResult):

    if isinstance(cwl, str):
        raise RuntimeError(f"{base_url.getulr()}: Expecting a process, found a string")

    if not isinstance(cwl, dict):
        return cwl

    if cwl.get("class") != "Workflow":
        return cwl

    for n, v in enumerate(cwl["steps"]):
        if isinstance(v, dict):
            sys.stderr.write(f"\n--\nRecursing into step {base_url.geturl()}:{v['id']}\n")

            _run = v.get("run")
            if isinstance(_run, str):
                v["run"], new_base_url = lib.load_linked_file(
                    base_url, _run, is_import=True
                )
            else:
                new_base_url = base_url

            v["run"] = pack_process(v["run"], new_base_url)

    return cwl


def add_missing_requirements(cwl: dict):
    requirements = cwl.get("requirements", [])
    present = set(req["class"] for req in requirements)

    def _add_req(_req_name: str):
        nonlocal requirements
        if _req_name not in present:
            requirements += [{"class": _req_name}]

    if cwl.get("class") == "Workflow":
        _add_req("SubworkflowFeatureRequirement")
    _add_req("InlineJavascriptRequirement")
    return cwl


def get_git_info(cwl_path: str) -> str:
    import subprocess, os

    source_str = cwl_path

    file_path_url = urllib.parse.urlparse(cwl_path)
    if file_path_url.scheme == "":
        source_path = pathlib.Path(cwl_path)
        os.chdir(source_path.parent)
        try:
            origin = (
                subprocess.check_output(["git", "config", "--get", "remote.origin.url"])
                .strip()
                .decode()
            )
            fpath = (
                subprocess.check_output(
                    ["git", "ls-files", "--full-name", source_path.name]
                )
                .strip()
                .decode()
            )
            changed = (
                subprocess.check_output(["git", "status", source_path.name, "-s"])
                .strip()
                .decode()
            )
            if changed == "":
                tag = (
                    subprocess.check_output(["git", "describe", "--always"])
                    .strip()
                    .decode()
                )
            else:
                tag = "(uncommitted file)"
            source_str = f"\nrepo: {origin}\nfile: {fpath}\ncommit: {tag}"

        except subprocess.CalledProcessError:
            pass

    return source_str


class AppIdCheck(enum.IntEnum):
    VALID = 0
    PATH_ERROR = 1
    ILLEGAL_CHARACTERS = 2


def validate_id(app_id: str):
    parts = app_id.split("/")
    if len(parts) != 3:
        return AppIdCheck.PATH_ERROR

    illegal = set(".!@#$%^&*()")
    if any((c in illegal) for c in parts[2]):
        return AppIdCheck.ILLEGAL_CHARACTERS

    return AppIdCheck.VALID


def print_usage():
    print(
        """Usage
   sbpack <profile> <id> <cwl>
 
where:
  <profile> refers to a SB platform profile as set in the SB API credentials file.
  <id> takes the form {user}/{project}/{app_id} which installs (or updates) 
       "app_id" located in "project" of "user".
  <cwl> is the path to the main CWL file to be uploaded. This can be a remote file.
"""
    )


def pack(cwl_path: str):
    sys.stderr.write(f"Packing {cwl_path}\n")
    file_path_url = urllib.parse.urlparse(cwl_path)

    cwl, full_url = lib.load_linked_file(
        base_url=file_path_url, link="", is_import=True)
    cwl = pack_process(cwl, full_url)
    return cwl


def main():

    logging.basicConfig()
    logger.setLevel(logging.INFO)
    print(
        f"\nsbpack v{__version__}\n"
        f"Upload CWL apps to any Seven Bridges powered platform\n"
        f"(c) Seven Bridges 2020\n"
    )

    if len(sys.argv) != 4:
        print_usage()
        exit(0)

    profile, appid, cwl_path = sys.argv[1:]

    app_id_check = validate_id(appid)
    if app_id_check == AppIdCheck.ILLEGAL_CHARACTERS:
        sys.stderr.write("Illegal characters in app id\n")
        return

    if app_id_check == AppIdCheck.PATH_ERROR:
        sys.stderr.write("Incorrect path for app id\n")
        return

    cwl = pack(cwl_path)

    api = lib.get_profile(profile)

    cwl[
        "sbg:revisionNotes"
    ] = f"Uploaded using sbpack v{__version__}. \nSource: {get_git_info(cwl_path)}"
    try:
        app = api.apps.get(appid)
        logger.debug("Creating revised app: {}".format(appid))
        return api.apps.create_revision(id=appid, raw=cwl, revision=app.revision + 1)
    except sbgerr.NotFound:
        logger.debug("Creating new app: {}".format(appid))
        return api.apps.install_app(id=appid, raw=cwl)


def print_local_usage():
    sys.stderr.write(
        """cwlpack <cwl>        
        """
    )


def localpack():
    logging.basicConfig()
    logger.setLevel(logging.INFO)

    if len(sys.argv) != 2:
        print_local_usage()
        exit(0)

    cwl_path = sys.argv[1]

    cwl = pack(cwl_path)
    fast_yaml.dump(cwl, sys.stdout)


if __name__ == "__main__":
    main()
