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


def pack_process(cwl: dict, base_url: urllib.parse.ParseResult):
    cwl = resolve_imports(cwl, base_url)
    cwl = resolve_linked_processes(cwl, base_url)
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
    print("sbpack <profile> <id> <cwl>")


def main():

    logger.setLevel(logging.INFO)
    logger.info(f"sbpack {__version__}")

    if len(sys.argv) != 4:
        print_usage()
        exit(0)

    profile, appid, cwl_path = sys.argv[1:]

    if not validate_id(appid):
        print("Illegal characters in app id")
        return

    file_path_url = urllib.parse.urlparse(cwl_path)
    if file_path_url.scheme == "":
        file_path_url = file_path_url._replace(scheme="file://")

    base_url = file_path_url._replace(path=str(pathlib.Path(file_path_url.path).parent))
    link = str(pathlib.Path(file_path_url.path).name)

    cwl, base_url = load_linked_file(base_url=base_url, link=link, is_import=True)
    cwl = pack_process(cwl, base_url)
    # fast_yaml.dump(cwl, sys.stdout)

    api = get_profile(profile)

    cwl["sbg:revisionNotes"] = f"Uploaded using sbpack. Source: {cwl_path}"
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
