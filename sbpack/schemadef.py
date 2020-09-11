"""
Valid forms of user defined types stored in external file

A single dictionary (tests/types/singletype.yml)
A list of dictionaries (e.g. tests/types/recursive.yml)
Types can refer to other types in the file
Names can not clash across files (This seems arbitrary and we allow that for packing)
Only records and arrays can be defined (https://github.com/common-workflow-language/cwl-v1.2/pull/14)
"""

import sys
import urllib.parse
from copy import deepcopy
from typing import Union

import sbpack.lib


def build_user_defined_type_dict(cwl: dict, base_url: urllib.parse.ParseResult):
    user_defined_types = {}

    schemadef = next((req for req in cwl.get("requirements", [])
                      if req.get("class") == "SchemaDefRequirement"), {})
    schema_list = schemadef.get("types", [])

    if not isinstance(schema_list, list):
        raise RuntimeError(f"In file {base_url.geturl()}: "
                           f"Schemadef types have to be a list\n"
                           f"Instead, got: {schema_list}")

    for schema in schema_list:
        if not isinstance(schema, dict):
            raise RuntimeError(f"In file {base_url.geturl()}: "
                               f"User type has to be a dict\n"
                               f"Instead, got: {schema}")

        if len(schema.keys()) == 1 and list(schema.keys())[0] == "$import":
            type_definition_list, this_url = \
                sbpack.lib.load_linked_file(base_url, schema["$import"], is_import=True)
            # This is always a list
            if isinstance(type_definition_list, dict):
                type_definition_list = [type_definition_list]
                # except when it isn't

            path_prefix = this_url.geturl() #sbpack.lib.normalized_path(schema["$import"], base_url).geturl()
            sys.stderr.write(f"Parsing Schemadefs for {path_prefix}\n")
            for v in type_definition_list:
                k = v.get("name")
                if k is None:
                    raise RuntimeError(f"In file {path_prefix} type missing name")
                user_defined_types[f"{path_prefix}#{k}"] = v

        else:
            path_prefix = base_url.geturl()
            user_defined_types[f"{path_prefix}#{schema.get('name')}"] = schema

    # sys.stderr.write(str(user_defined_types))
    # sys.stderr.write("\n")

    return user_defined_types


# port = "input" or "output"
def inline_types(cwl: dict, port: str, base_url: urllib.parse.ParseResult, user_defined_types: dict):
    cwl[port] = [_inline_type(v, base_url, user_defined_types) for v in cwl[port]]
    return cwl


def _inline_type(v, base_url, user_defined_types):
    try:
        _inline_type.type_name_uniq_id += 1
    except AttributeError:
        _inline_type.type_name_uniq_id = 1

    if isinstance(v, str):

        # Handle syntactic sugar
        if v.endswith("[]"):
            return {
                "type": "array",
                "items": _inline_type(v[:-2], base_url, user_defined_types)
            }

        if v.endswith("?"):
            return [
                    "null",
                    _inline_type(v[:-1], base_url, user_defined_types)
            ]

        if v in sbpack.lib.built_in_types:
            return v

        if "#" not in v:
            path_prefix = base_url
            path_suffix = v
        else:
            parts = v.split("#")
            path_prefix = sbpack.lib.resolved_path(base_url, parts[0])
            path_suffix = parts[1]

        path = f"{path_prefix.geturl()}#{path_suffix}"

        if path not in user_defined_types:
            raise RuntimeError(f"Could not find type '{path}'")
        else:
            resolve_type = deepcopy(user_defined_types[path])
            # resolve_type.pop("name", None) # Should work, but cwltool complains
            resolve_type["name"] = f"user_type_{_inline_type.type_name_uniq_id}"
            return _inline_type(resolve_type, path_prefix, user_defined_types)

    elif isinstance(v, list):
        return [_inline_type(_v, base_url, user_defined_types) for _v in v]

    elif isinstance(v, dict):
        _type = v.get("type")
        if _type is None:
            raise sbpack.lib.MissingTypeName(
                f"In file {base_url.geturl()}, type {_type.get('name')} is missing type name")

        elif _type == "enum":
            return v

        elif _type == "array":
            if "items" not in v:
                raise sbpack.lib.ArrayMissingItems(
                    f"In file {base_url.geturl()}, array type {_type.get('name')} is missing 'items'")

            v["items"] = _inline_type(v["items"], base_url, user_defined_types)
            return v

        elif _type == "record":
            if "fields" not in v:
                raise sbpack.lib.RecordMissingFields(
                    f"In file {base_url.geturl()}, record type {_type.get('name')} is missing 'fields'")

            fields = sbpack.lib.normalize_to_list(v["fields"], key_field="name", value_field="type")
            v["fields"] = [
                _inline_type(_f, base_url, user_defined_types)
                for _f in fields
            ]
            return v

        elif _type in sbpack.lib.built_in_types:
            return v

        else:
            v["type"] = _inline_type(_type, base_url, user_defined_types)
            return v

    else:
        raise RuntimeError("Found a type sbpack can not understand")
