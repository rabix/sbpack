import urllib.parse

from sbpack.pack import pack


def test_recursive_type_resolution():

    cwl = pack("tools/clt1.cwl")

    simple_record = cwl.get("inputs").get("in2").get("type")

    assert simple_record.get("type") == "record"
