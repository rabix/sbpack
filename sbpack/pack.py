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
import json

from ruamel.yaml import YAML

import sevenbridges as sbg
import sevenbridges.errors as sbgerr

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
    cwl = dictify_requirements(cwl)
    cwl = normalize_sources(cwl)
    cwl = resolve_schemadefs(cwl, base_url)
    cwl = resolve_imports(cwl, base_url)
    cwl = resolve_linked_processes(cwl, base_url)
    cwl = add_missing_requirements(cwl)
    return cwl


def dictify_requirements(cwl: dict):
    _requirements = cwl.get("requirements")
    if _requirements is None or not isinstance(_requirements, (list, dict)):
        return cwl

    if isinstance(_requirements, list):
        new_requirements = {
            _req.get("class"): _req
            for _req in _requirements
            if _req.get("class") is not None
        }
    else:
        new_requirements = {k: _req for k, _req in _requirements.items()}
    cwl["requirements"] = new_requirements
    return cwl


def normalize_sources(cwl: dict):
    if cwl.get("class") != "Workflow":
        return cwl

    _steps = cwl.get("steps")
    if not isinstance(_steps, (list, dict)):
        return cwl

    for _step in _steps.values() if isinstance(_steps, dict) else _steps:
        if not isinstance(_step, dict):
            continue

        _inputs = _step.get("in")
        if not isinstance(_step, (list, dict)):
            continue

        for k, _input in (
            _inputs.items() if isinstance(_inputs, dict) else enumerate(_inputs)
        ):
            if isinstance(_input, str):
                _inputs[k] = _normalize(_input)
            elif isinstance(_input, dict):
                _src = _input.get("source")
                if isinstance(_src, str):
                    _input["source"] = _normalize(_input["source"])

    _outputs = cwl.get("outputs")
    for k, _output in (
        _outputs.items() if isinstance(_outputs, dict) else enumerate(_outputs)
    ):
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
    user_defined_types = build_user_defined_type_dict(cwl, base_url)
    cwl = _remove_schemadef(cwl)
    cwl["inputs"] = resolve_user_defined_types(
        cwl.get("inputs"), user_defined_types, base_url
    )
    cwl["outputs"] = resolve_user_defined_types(
        cwl.get("outputs"), user_defined_types, base_url
    )
    return cwl


def build_user_defined_type_dict(cwl: dict, base_url: urllib.parse.ParseResult):
    _requirements = cwl.get("requirements")
    if _requirements is None or not isinstance(_requirements, (list, dict)):
        return {}

    for k, req in _requirements.items():
        if k == "SchemaDefRequirement":
            return _build_user_defined_type_dict(req, base_url)

    return {}


def _build_user_defined_type_dict(
    requirements: list, base_url: urllib.parse.ParseResult
):
    user_type_dict = {}

    if isinstance(requirements, dict):
        requirements = requirements.get("types")

    if not isinstance(requirements, list):
        return user_type_dict

    for _req in requirements:
        if not isinstance(_req, dict):
            continue

        if len(_req.keys()) == 1 and list(_req.keys())[0] == "$import":
            _user_types, _ = load_linked_file(base_url, _req["$import"], is_import=True)
            _this_type_dict = {}
            for _user_type in (
                _user_types if isinstance(_user_types, list) else [_user_types]
            ):
                _name = _user_type.get("name")
                if _name is None:
                    logger.error(f"Missing name in {_req['$import']}")
                    continue
                _this_type_dict[_name] = _user_type

            user_type_dict[
                _normalized_path(_req["$import"], base_url)
            ] = _this_type_dict
        else:
            user_type_dict[_req.get("name")] = _req

    return user_type_dict


def resolve_user_defined_types(
    ports: dict, user_defined_types: dict, base_url: urllib.parse.ParseResult
):
    if ports is None:
        return {}

    for k, _inp in ports.items() if isinstance(ports, dict) else enumerate(ports):
        if isinstance(_inp, dict) and "type" in _inp:
            _inp["type"] = _resolve_type(_inp["type"], user_defined_types, base_url)

        elif isinstance(_inp, str):
            ports[k] = {"type": _resolve_type(_inp, user_defined_types, base_url)}

    return ports


def _resolve_type(
    _type: str, user_defined_types: dict, base_url: urllib.parse.ParseResult
):
    if not isinstance(_type, str):
        return _type

    if "#" not in _type:
        return _type

    type_path, type_name = _type.split("#")

    norm_type_path = _normalized_path(type_path, base_url)

    if norm_type_path not in user_defined_types:
        logger.error(f"Undefined type: {_type}")
        return _type

    if type_name not in user_defined_types[norm_type_path]:
        logger.error(f"Undefined type: {_type}")
        return _type

    return user_defined_types[norm_type_path][type_name]


def _remove_schemadef(cwl: dict):
    _requirements = cwl.get("requirements")
    if _requirements is None or not isinstance(_requirements, (list, dict)):
        return cwl

    cwl["requirements"] = {
        k: _req for k, _req in _requirements.items() if k != "SchemaDefRequirement"
    }
    return cwl


def _normalized_path(link: str, base_url: urllib.parse.ParseResult):
    link_url = urllib.parse.urlparse(link)
    if link_url.scheme in ["file://", ""]:
        new_url = base_url._replace(
            path=str((pathlib.Path(base_url.path) / pathlib.Path(link)).resolve())
        )
    else:
        new_url = link_url

    return new_url


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
                    cwl[k], this_base_url = load_linked_file(
                        base_url, v[_k], is_import=_k == "$import"
                    )

        cwl[k] = resolve_imports(cwl[k], base_url)

    return cwl


def resolve_linked_processes(cwl: dict, base_url: urllib.parse.ParseResult):

    if isinstance(cwl, str):
        # This is an exception for symbolic links.
        logger.warning(base_url.geturl())
        logger.warning(cwl)
        logger.warning(
            "Expecting a process, found a string. Treating this as a symbolic link."
        )
        cwl, this_base_url = load_linked_file(base_url, cwl, is_import=True)
        cwl = pack_process(cwl, this_base_url)
        return cwl

    if not isinstance(cwl, dict):
        return cwl

    if cwl.get("class") != "Workflow":
        return cwl

    steps = cwl.get("steps")
    if isinstance(steps, dict):
        itr = steps.items()
    elif isinstance(steps, list):
        itr = [(n, v) for n, v in enumerate(steps)]
    else:
        return cwl

    for k, v in itr:
        if isinstance(v, dict):
            _run = v.get("run")
            if isinstance(_run, str):
                v["run"], this_base_url = load_linked_file(
                    base_url, _run, is_import=True
                )
            else:
                this_base_url = base_url

            v["run"] = pack_process(v["run"], this_base_url)

    return cwl


def load_linked_file(base_url: urllib.parse.ParseResult, link: str, is_import=False):

    link_url = urllib.parse.urlparse(link)
    if link_url.scheme in ["file://", ""]:
        new_url = base_url._replace(
            path=str((pathlib.Path(base_url.path) / pathlib.Path(link)).resolve())
        )

    else:
        new_url = link_url

    contents = urllib.request.urlopen(new_url.geturl()).read().decode("utf-8")
    new_base_url = new_url._replace(path=str(pathlib.Path(new_url.path).parent))

    if is_import:
        _node = fast_yaml.load(contents)

    else:
        _node = contents

    return _node, new_base_url


def add_missing_requirements(cwl: dict):
    def _add_req(_req_name: str):
        if _req_name not in _requirements:
            _requirements[_req_name] = {}

    _requirements = cwl.get("requirements", {})
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


def get_profile(profile):
    api = sbg.Api(config=sbg.Config(profile))
    # Least disruptive way to add in our user agent
    api.headers["User-Agent"] = "sbpack/{} via {}".format(
        __version__, api.headers["User-Agent"]
    )
    return api


def validate_id(app_id: str):
    parts = app_id.split("/")
    if len(parts) != 3:
        return False

    illegal = set(".!@#$%^&*()")
    return not any((c in illegal) for c in parts[2])


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
    file_path_url = urllib.parse.urlparse(cwl_path)
    if file_path_url.scheme == "":
        file_path_url = file_path_url._replace(scheme="file://")

    base_url = file_path_url._replace(path=str(pathlib.Path(file_path_url.path).parent))
    link = str(pathlib.Path(file_path_url.path).name)

    cwl, base_url = load_linked_file(base_url=base_url, link=link, is_import=True)
    cwl = pack_process(cwl, base_url)
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

    if not validate_id(appid):
        print("Illegal characters in app id")
        return

    cwl = pack(cwl_path)

    api = get_profile(profile)

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
    print(
        """cwlpack <cwl>        
        """,
        sys.stderr,
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
