"""Tests for `cwlpack` a.k.a the localpack() function."""

from sbpack.pack import _localpack
from sbpack.schemadef import _inline_type
from pytest import CaptureFixture

def test_json_output(capsys: CaptureFixture[str]) -> None:
    _inline_type.type_name_uniq_id = 0
    _inline_type.type_names = set()
    cwl = _localpack(["--json", "workflows/wf6.cwl"])
    with open("workflows/wf6.json") as handle:
        expected = handle.read()

    assert capsys.readouterr().out == expected


def test_add_ids(capsys: CaptureFixture[str]) -> None:
    """Confirm proper placement with --add-ids."""
    _inline_type.type_name_uniq_id = 0
    _inline_type.type_names = set()
    cwl = _localpack(["--add-ids", "--json", "workflows/wf6.cwl"])
    with open("workflows/wf6_with_ids.json") as handle:
        expected = handle.read()

    assert capsys.readouterr().out == expected
