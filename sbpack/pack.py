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

fast_yaml = YAML(typ='safe')


def get_inner_dict(cwl: dict, path: list):
    if len(path) == 0:
        return cwl

    if isinstance(cwl, dict):
        _v = cwl.get(path[0]["key"])
        if _v is not None:
            return get_inner_dict(_v, path[1:])

    elif isinstance(cwl, list): # Going to assume this is a map expressed as list
        for _v in cwl:
            if isinstance(_v, dict):
                if _v.get(path[0]["key_field"]) == path[0]["key"]:
                    return get_inner_dict(_v, path[1:])

    return None


def pack_process(cwl: dict, base_url: urllib.parse.ParseResult):
    cwl = normalize_sources(cwl)
    cwl = resolve_schemadefs(cwl, base_url)
    cwl = resolve_imports(cwl, base_url)
    cwl = resolve_linked_processes(cwl, base_url)
    cwl = handle_user_defined_types(cwl, base_url)
    return cwl


def normalize_sources(cwl: dict):
    if cwl.get("class") != "Workflow":
        return cwl

    _steps = cwl.get("steps")
    if not isinstance(_steps, (list, dict)):
        return cwl

    for _step in (_steps.values() if isinstance(_steps, dict) else _steps):
        if not isinstance(_step, dict):
            continue

        _inputs = _step.get("in")
        if not isinstance(_step, (list, dict)):
            continue

        for k, _input in (_inputs.items() if isinstance(_inputs, dict) else enumerate(_inputs)):
            if isinstance(_input, str):
                _inputs[k] = _normalize(_input)
            elif isinstance(_input, dict):
                _src = _input.get("source")
                if isinstance(_src, str):
                    _input["source"] = _normalize(_input["source"])

    _outputs = cwl.get("outputs")
    for k, _output in (_outputs.items() if isinstance(_outputs, dict) else enumerate(_outputs)):
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
    _req = cwl.get("requirements")
    if _req is None or not isinstance(_req, (list, dict)):
        return cwl

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
                    cwl[k], this_base_url = load_linked_file(base_url, v[_k], is_import=_k == "$import")

        cwl[k] = resolve_imports(cwl[k], base_url)

    return cwl


def resolve_linked_processes(cwl: dict, base_url: urllib.parse.ParseResult):

    if isinstance(cwl, str):
        # This is an exception for symbolic links.
        logger.warning(base_url.geturl())
        logger.warning(cwl)
        logger.warning("Expecting a process, found a string. Treating this as a symbolic link.")
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
                v["run"], this_base_url = load_linked_file(base_url, _run, is_import=True)
            else:
                this_base_url = base_url

            v["run"] = pack_process(v["run"], this_base_url)

    return cwl


# For now, this does not verify that you have correctly $import -ed the type file
def handle_user_defined_types(cwl, base_url: urllib.parse.ParseResult):
    if isinstance(cwl, dict):
        for k in cwl.keys():
            if k == "type":
                if "#" in cwl[k]:
                    pass
    return cwl


def load_linked_file(base_url: urllib.parse.ParseResult, link: str, is_import=False):

    link_url = urllib.parse.urlparse(link)
    if link_url.scheme in ["file://", ""]:
        new_url = base_url._replace(
            path=str((pathlib.Path(base_url.path) / pathlib.Path(link)).resolve()))

    else:
        new_url = link_url

    contents = urllib.request.urlopen(new_url.geturl()).read().decode('utf-8')
    new_base_url = new_url._replace(path=str(pathlib.Path(new_url.path).parent))

    if is_import:
        _node = fast_yaml.load(contents)

    else:
        _node = contents

    return _node, new_base_url


def get_profile(profile):
    api = sbg.Api(config=sbg.Config(profile))
    # Least disruptive way to add in our user agent
    api.headers["User-Agent"] = "sbpack/{} via {}".format(__version__, api.headers["User-Agent"])
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
""")


def pack(cwl_path: str):
    file_path_url = urllib.parse.urlparse(cwl_path)
    if file_path_url.scheme == "":
        file_path_url = file_path_url._replace(scheme="file://")

    base_url = file_path_url._replace(path=str(pathlib.Path(file_path_url.path).parent))
    link = str(pathlib.Path(file_path_url.path).name)

    cwl, base_url = load_linked_file(base_url=base_url, link=link, is_import=True)
    cwl = pack_process(cwl, base_url)
    return cwl


def handle_hash_in_source(cwl):
    if isinstance(cwl, dict):
        for k in cwl.keys():
            if k in ["source", "outputSource"]:
                if cwl[k][0] == "#":
                    cwl[k] = cwl[k][1:]
            else:
                handle_hash_in_source(cwl[k])

    elif isinstance(cwl, list):
        for l in cwl:
            handle_hash_in_source(l)


def main():

    logging.basicConfig()
    logger.setLevel(logging.INFO)
    print(f"\nsbpack v{__version__}\n"
          f"Upload CWL apps to any Seven Bridges powered platform\n"
          f"(c) Seven Bridges 2020\n")

    if len(sys.argv) != 4:
        print_usage()
        exit(0)

    profile, appid, cwl_path = sys.argv[1:]

    if not validate_id(appid):
        print("Illegal characters in app id")
        return

    cwl = pack(cwl_path)
    handle_hash_in_source(cwl)
    # fast_yaml.dump(cwl, sys.stdout)

    api = get_profile(profile)

    cwl["sbg:revisionNotes"] = f"Uploaded using sbpack v{__version__}. \nSource: {cwl_path}"
    try:
        app = api.apps.get(appid)
        logger.debug("Creating revised app: {}".format(appid))
        return api.apps.create_revision(
            id=appid,
            raw=cwl,
            revision=app.revision + 1
        )
    except sbgerr.NotFound:
        logger.debug("Creating new app: {}".format(appid))
        return api.apps.install_app(
            id=appid,
            raw=cwl
        )


if __name__ == "__main__":
    main()
