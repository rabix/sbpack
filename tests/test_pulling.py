import subprocess
import pathlib
import tempfile


def test_pull():

    with tempfile.TemporaryDirectory() as td:
        out_dir = pathlib.Path(td)
        subprocess.run([
            "sbpull",
            "--unpack",
            ".",
            "admin/sbg-public-data/eqtl-analysis-with-fastqtl-gtex-v7/",
            str(out_dir / "eqtl.cwl"),
        ], check=True)

        assert (out_dir / "eqtl.cwl").exists()
        assert (out_dir / "eqtl.cwl.steps").exists()
