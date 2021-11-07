"""Tests for `cwlpack` a.k.a the localpack() function."""

from sbpack.pack import _localpack
from pytest import CaptureFixture

def test_json_output(capsys: CaptureFixture[str]) -> None:
    cwl = _localpack(["--json", "workflows/wf6.cwl"])
    with open("workflows/wf6.json") as handle:
        expected = handle.read()

    assert capsys.readouterr().out == expected
