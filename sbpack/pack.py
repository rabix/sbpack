"""
The link resolution is as follows:

We always have two components: the base and the link
If the link is a url or absolute path it is what is used to fetch the data.
If the link is a relative path it is combined with the base and that is what is
used to fetch data
"""

#  Copyright (c) 2021 Michael R. Crusoe
#  Copyright (c) 2020 Seven Bridges. See LICENSE

import argparse
import os
import sys
import pathlib
import urllib.parse
import urllib.request
from typing import Union
from copy import deepcopy
import json
import enum

from ruamel.yaml import YAML
from packaging import version
import sevenbridges.errors as sbgerr

import sbpack.schemadef as schemadef
import sbpack.lib as lib

from sbpack.version import __version__

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


def pack_process(
    cwl: dict,
    base_url: urllib.parse.ParseResult,
    cwl_version: str,
    parent_user_defined_types=None,
    add_ids: bool = False,
):
    cwl = listify_everything(cwl)
    cwl = normalize_sources(cwl)
    cwl, user_defined_types = \
        load_schemadefs(cwl, base_url, parent_user_defined_types)
    cwl = resolve_schemadefs(cwl, base_url, user_defined_types)
    cwl = resolve_imports(cwl, base_url)
    cwl = resolve_steps(
        cwl,
        base_url,
        cwl.get("cwlVersion", cwl_version),
        user_defined_types,
        add_ids=add_ids,
    )
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


def load_schemadefs(cwl: dict, base_url: urllib.parse.ParseResult,
                    parent_user_defined_types=None):
    user_defined_types = schemadef.build_user_defined_type_dict(cwl, base_url)
    if parent_user_defined_types is not None:
        user_defined_types.update(parent_user_defined_types)

    cwl["requirements"] = [
        req
        for req in cwl.get("requirements", [])
        if req.get("class") != "SchemaDefRequirement"
    ]

    return cwl, user_defined_types


def resolve_schemadefs(cwl: dict, base_url: urllib.parse.ParseResult,
                       user_defined_types):
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


def resolve_steps(
    cwl: dict,
    base_url: urllib.parse.ParseResult,
    cwl_version: str,
    parent_user_defined_types=None,
    add_ids: bool = False,
):

    if isinstance(cwl, str):
        raise RuntimeError(f"{base_url.getulr()}: Expecting a process, found a string")

    if not isinstance(cwl, dict):
        return cwl

    if cwl.get("class") != "Workflow":
        return cwl

    workflow_id = cwl.get("id", os.path.basename(base_url.path))
    for n, v in enumerate(cwl["steps"]):
        if isinstance(v, dict):
            sys.stderr.write(f"\n--\nRecursing into step {base_url.geturl()}:{v['id']}\n")

            _run = v.get("run")
            if isinstance(_run, str):
                v["run"], new_base_url = lib.load_linked_file(
                    base_url, _run, is_import=True
                )
                v["run"] = pack_process(
                    v["run"],
                    new_base_url,
                    cwl.get("cwlVersion", cwl_version),
                    add_ids=add_ids,
                )
                if "id" not in v["run"] and add_ids:
                    v["run"][
                        "id"
                    ] = f"{workflow_id}@step_{v['id']}@{os.path.basename(_run)}"
            else:
                v["run"] = pack_process(
                    v["run"],
                    base_url,
                    cwl.get("cwlVersion", cwl_version),
                    parent_user_defined_types,
                    add_ids=add_ids,
                )
                if "id" not in v["run"] and add_ids:
                    v["run"]["id"] = f"{workflow_id}@step_{v['id']}@run"
            if "cwlVersion" in v["run"]:
                parent_version = version.parse(
                    cwl.get("cwlVersion", cwl_version).strip("v")
                )
                this_version = version.parse(v["run"]["cwlVersion"].strip("v"))
                if this_version > parent_version:
                    cwl["cwlVersion"] = v["run"]["cwlVersion"]
                    # not really enough, but hope for the best

    return cwl


def add_missing_requirements(cwl: dict):
    requirements = cwl.get("requirements", [])
    present = {req["class"] for req in requirements}

    def _add_req(_req_name: str):
        nonlocal requirements
        if _req_name not in present:
            requirements += [{"class": _req_name}]

    if cwl.get("class") == "Workflow":
        sub_worflow = False
        for step in cwl["steps"]:
            if step["run"]["class"] == "Workflow":
                sub_worflow = True
                break
        if sub_worflow:
            _add_req("SubworkflowFeatureRequirement")
    return cwl


def no_non_sbg_tag(val: str):
    if ":" not in val:
        return True

    if val.startswith("sbg:"):
        return True

    return False


def filter_out_non_sbg_tags(cwl: Union[list, dict]):

    if isinstance(cwl, dict):
        return {
            k: filter_out_non_sbg_tags(v)
            for k, v in cwl.items()
            if no_non_sbg_tag(k)
        }

    elif isinstance(cwl, list):
        return [filter_out_non_sbg_tags(c) for c in cwl]
    
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


def pack(cwl_path: str, filter_non_sbg_tags=False, add_ids=False):
    sys.stderr.write(f"Packing {cwl_path}\n")
    file_path_url = urllib.parse.urlparse(cwl_path)

    cwl, full_url = lib.load_linked_file(
        base_url=file_path_url, link="", is_import=True)
    if "$graph" in cwl:
        # assume already packed
        return cwl
    cwl = pack_process(cwl, full_url, cwl["cwlVersion"], add_ids=add_ids)
    if add_ids and "id" not in cwl:
        cwl["id"] = os.path.basename(file_path_url.path)
    if filter_non_sbg_tags:
        cwl = filter_out_non_sbg_tags(cwl)

    return cwl


def main():

    logging.basicConfig()
    logger.setLevel(logging.INFO)
    print(
        f"\nsbpack v{__version__}\n"
        f"Upload CWL apps to any Seven Bridges powered platform\n"
        f"(c) Seven Bridges 2020\n"
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("profile", help="SB platform profile as set in the SB API credentials file.")
    parser.add_argument("appid", help="Takes the form {user}/{project}/{app_id}.")
    parser.add_argument("cwl_path", help="Path  or URL to the main CWL file to be uploaded.")
    parser.add_argument("--filter-non-sbg-tags",
                        action="store_true",
                        help="Filter out custom tags that are not 'sbg:'")

    args = parser.parse_args()

    profile, appid, cwl_path = args.profile, args.appid, args.cwl_path

    app_id_check = validate_id(appid)
    if app_id_check == AppIdCheck.ILLEGAL_CHARACTERS:
        sys.stderr.write("Illegal characters in app id\n")
        return

    if app_id_check == AppIdCheck.PATH_ERROR:
        sys.stderr.write("Incorrect path for app id\n")
        return

    cwl = pack(cwl_path, filter_non_sbg_tags=args.filter_non_sbg_tags)

    api = lib.get_profile(profile)

    cwl[
        "sbg:revisionNotes"
    ] = f"Uploaded using sbpack v{__version__}. \nSource: {get_git_info(cwl_path)}"
    try:
        app = api.apps.get(appid)
        logger.debug(f"Creating revised app: {appid}")
        api.apps.create_revision(id=appid, raw=cwl, revision=app.revision + 1)
    except sbgerr.NotFound:
        logger.debug(f"Creating new app: {appid}")
        api.apps.install_app(id=appid, raw=cwl)


def localpack():
    _localpack(sys.argv[1:])

def _localpack(args):
    logging.basicConfig()
    logger.setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "cwl_path", help="Path  or URL to the main CWL file to be uploaded."
    )
    parser.add_argument(
        "--json", action="store_true", help="Output in JSON format, not YAML."
    )
    parser.add_argument(
        "--add-ids",
        action="store_true",
        help='Insert "id" fields in processes, if they are missing.',
    )
    parser.add_argument(
        "--filter-non-sbg-tags",
        action="store_true",
        help="Filter out custom tags that are not 'sbg:'",
    )

    args = parser.parse_args(args)

    cwl_path = args.cwl_path

    cwl = pack(
        cwl_path, filter_non_sbg_tags=args.filter_non_sbg_tags, add_ids=args.add_ids
    )
    if args.json:
        json.dump(cwl, sys.stdout, indent=4)
    else:
        fast_yaml.dump(cwl, sys.stdout)


if __name__ == "__main__":
    main()
