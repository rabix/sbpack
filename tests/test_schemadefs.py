import urllib.parse

from sbpack.pack import pack


def _find(l: list, key: str, val: str):
    return next(_x for _x in l if _x[key] == val)


def test_recursive_type_resolution():
    cwl = pack("tools/clt1.cwl")
    simple_record = _find(cwl.get("inputs"), "id", "in2").get("type")
    assert simple_record.get("type") == "record"
