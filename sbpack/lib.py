from typing import Union
from copy import deepcopy
import urllib.parse
import urllib.request
import pathlib
import sys

import sevenbridges as sbg

from .version import __version__

from ruamel.yaml.parser import ParserError
from ruamel.yaml.scanner import ScannerError
from ruamel.yaml import YAML
fast_yaml = YAML(typ="safe")


class MissingTypeName(BaseException):
    pass


class MissingCWLType(BaseException):
    pass


class RecordMissingFields(BaseException):
    pass


class ArrayMissingItems(BaseException):
    pass


class MissingKeyField(BaseException):
    pass


built_in_types = ["null", "boolean", "int", "long", "float", "double", "string",  "File", "Directory", "stdout", "stderr"]
magic_string = "##sbpack_rename_user_type##"


def normalize_to_map(obj: Union[list, dict], key_field: str):
    if isinstance(obj, dict):
        return deepcopy(obj)
    elif isinstance(obj, list):
        map_obj = {}
        for v in obj:
            if not isinstance(v, dict):
                raise RuntimeError("Expecting a dict here")
            k = v.get(key_field)
            if k is None:
                raise MissingKeyField(key_field)
            v.pop(key_field, None)
            map_obj[k] = v
        return map_obj
    else:
        raise RuntimeError("Expecting a dictionary or a list here")


def normalize_to_list(obj: Union[list, dict], key_field: str, value_field: str):
    if isinstance(obj, list):
        return deepcopy(obj)
    elif isinstance(obj, dict):
        map_list = []
        for k, v in obj.items():
            if not isinstance(v, dict):
                if value_field is None:
                    raise RuntimeError(f"Expecting a dict here, got {v}")
                v = {value_field: v}
            v.update({key_field: k})
            map_list += [v]
        return map_list
    else:
        raise RuntimeError("Expecting a dictionary or a list here")


# To deprecate
def normalized_path(link: str, base_url: urllib.parse.ParseResult):
    link_url = urllib.parse.urlparse(link)
    if link_url.scheme in ["file://", ""]:
        new_url = base_url._replace(
            path=str((pathlib.Path(base_url.path) / pathlib.Path(link)).resolve())
        )
    else:
        new_url = link_url

    return new_url


def resolved_path(base_url: urllib.parse.ParseResult, link: str):
    """
    Given a base_url ("this document") and a link ("string in this document")
    return a new url (urllib.parse.ParseResult) that allows us to retrieve the
    linked document. This function will 
    1. Resolve the path, which means dot and double dot components are resolved
    2. Use the OS appropriate path resolution for local paths, and network
       apropriate resolution for network paths
    """
    link_url = urllib.parse.urlparse(link)
    # The link will always Posix

    if link_url.scheme == "file://":
        # Absolute local path
        new_url = urllib.parse.ParseResult(link_url)

    elif link_url.scheme == "":
        # Relative path, can be local or remote
        if base_url.scheme in ["file://", ""]:
            # Local relative path
            if link == "":
                new_url = base_url
            else:
                new_url = base_url._replace(
                    path=str((pathlib.Path(base_url.path).parent / pathlib.Path(link)).resolve())
                )

        else:
            # Remote relative path
            new_url = urllib.parse.urlparse(urllib.parse.urljoin(base_url.geturl(), link_url.path))
            # We need urljoin because we need to resolve relative links in a
            # platform independent manner

    else:
        # Absolute remote path
        new_url = urllib.parse.ParseResult(link_url)

    return new_url


def load_linked_file(base_url: urllib.parse.ParseResult, link: str, is_import=False):

    new_url = resolved_path(base_url, link)

    if new_url.scheme in ["file://", ""]:
        contents = pathlib.Path(new_url.path).open().read()
    else:
        try:
            contents = urllib.request.urlopen(new_url.geturl()).read().decode("utf-8")
        except urllib.error.HTTPError as e:
            e.msg += f"\n===\nCould not find linked file: {new_url.geturl()}\n===\n"
            raise SystemExit(e)

    if _is_github_symbolic_link(new_url, contents):
        # This is an exception for symbolic links on github
        sys.stderr.write(
            f"{new_url.geturl()}: found file-like string in contents.\n" 
            f"Treating as github symbolic link to {contents}\n")
        return load_linked_file(new_url, contents, is_import=is_import)

    if is_import:
        try:
            _node = fast_yaml.load(contents)
        except ParserError as e:
            e.context = f"\n===\nMalformed file: {new_url.geturl()}\n===\n" + e.context
            raise SystemExit(e)
        except ScannerError as e:
            e.problem = f"\n===\nMalformed file: {new_url.geturl()}\n===\n" + e.problem
            raise SystemExit(e)

    else:
        _node = contents

    return _node, new_url


def _is_github_symbolic_link(base_url: urllib.parse.ParseResult, contents: str):
    """Look for remote path with contents that is a single line with no new
    line with an extension."""
    if base_url.scheme in ["file://", ""]:
        return False

    idx = contents.find("\n")
    if idx > -1:
        return False

    if "." not in contents:
        return False

    return True


def get_profile(profile):
    if profile == ".":
        api = sbg.Api()
    else:
        api = sbg.Api(config=sbg.Config(profile))
    # Least disruptive way to add in our user agent
    api.headers["User-Agent"] = "sbpack/{} via {}".format(
        __version__, api.headers["User-Agent"]
    )
    return api
