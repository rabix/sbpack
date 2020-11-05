import urllib.parse

from sbpack.pack import pack


def _find(l: list, key: str, val: str):
    return next(_x for _x in l if _x[key] == val)


def test_recursive_type_resolution():
    cwl = pack("tools/clt1.cwl")
    simple_record = _find(cwl.get("inputs"), "id", "in2").get("type")
    assert simple_record.get("type") == "record"


def test_parent_type_resolution():
    cwl = pack("workflows/wf6.cwl")
    _type = cwl.get("steps")[0].get("run").get("inputs")[0].get("type")
    assert _type.get("type") == "record"
