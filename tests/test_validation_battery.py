import subprocess
import sys
import pathlib

import pytest

from ruamel.yaml import YAML

from sbpack.pack import pack

fast_yaml = YAML(typ="safe")


def validate(fname):
    try:
        subprocess.run(["cwltool", "--validate", fname], check=True)
        return True
    except subprocess.CalledProcessError as e:
        sys.stderr.write("Could not validate fname")
        return False


@pytest.mark.parametrize(
    ('f',),[("tools/clt1.cwl",), ("tools/clt2.cwl",), ("tools/clt3.cwl",), ("remote-cwl/tool1.cwl",),
            ("remote-cwl/tool2.cwl",), ("remote-cwl/wf1.cwl",)])
def test_validation(f):
    fin = pathlib.Path(f)
    cwl = pack(str(fin))
    fpacked = pathlib.Path(fin.stem + "-packed.cwl")
    with fpacked.open("w") as fout:
        fast_yaml.dump(cwl, fout)
    assert validate(fpacked)
    fpacked.unlink()
