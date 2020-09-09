from typing import Union
from copy import deepcopy
import urllib.parse
import urllib.request
import pathlib

import sevenbridges as sbg
import sevenbridges.errors as sbgerr

from .version import __version__

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


built_in_types = ["null", "boolean", "int", "long", "float", "double", "string",  "File", "Directory"]
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


def normalized_path(link: str, base_url: urllib.parse.ParseResult):
    link_url = urllib.parse.urlparse(link)
    if link_url.scheme in ["file://", ""]:
        new_url = base_url._replace(
            path=str((pathlib.Path(base_url.path) / pathlib.Path(link)).resolve())
        )
    else:
        new_url = link_url

    return new_url


def load_linked_file(base_url: urllib.parse.ParseResult, link: str, is_import=False):

    link_url = urllib.parse.urlparse(link)

    if link_url.scheme == "file://":
        # Absolute local path
        new_url = urllib.parse.ParseResult(link_url)
        new_base_url = new_url._replace(path=str(pathlib.Path(new_url.path).parent))

    elif link_url.scheme == "":
        # Relative path, can be local or remote
        if base_url.scheme in ["file://", ""]:
            # Local relative path
            new_url = base_url._replace(
                path=str((pathlib.Path(base_url.path) / pathlib.Path(link)).resolve())
            )
            new_base_url = new_url._replace(path=str(pathlib.Path(new_url.path).parent))

        else:
            # Remote relative path
            new_url = urllib.parse.urlparse(urllib.parse.urljoin(base_url.geturl() + "/dummy", link_url.path))
            new_base_url = new_url._replace(path=str(pathlib.PurePosixPath(new_url.path).parent))
            # We need urljoin because we need to resolve relative links in a
            # platform independent manner
            # We add the last "/dummy" component because of the way urljoin works

    else:
        # Absolute remote path
        new_url = urllib.parse.ParseResult(link_url)
        new_base_url = new_url._replace(path=str(pathlib.PurePosixPath(new_url.path).parent))
        # PurePosixPath ensures that remote paths don't get backward slashes on
        # windows systems

    if new_url.scheme in ["file://", ""]:
        contents = pathlib.Path(new_url.path).open().read()
    else:
        contents = urllib.request.urlopen(new_url.geturl()).read().decode("utf-8")

    if is_import:
        _node = fast_yaml.load(contents)

    else:
        _node = contents

    return _node, new_base_url, new_url


def get_profile(profile):
    api = sbg.Api(config=sbg.Config(profile))
    # Least disruptive way to add in our user agent
    api.headers["User-Agent"] = "sbpack/{} via {}".format(
        __version__, api.headers["User-Agent"]
    )
    return api
