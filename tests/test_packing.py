from sbpack.pack import pack


def test_port_normalization():
    cwl = pack("remote-cwl/wf1.cwl")
    assert cwl.get("steps").get("s1").get("in").get("in1") == "in1"

    cwl = pack("wf2.cwl")
    assert cwl.get("steps").get("s1").get("in")[0].get("source") == "in1"
    assert cwl.get("outputs").get("out1").get("outputSource") == "s2/out1"


def test_include():
    cwl = pack("remote-cwl/tool1.cwl")
    assert "arguments" in cwl
    assert isinstance(cwl.get("arguments"), list)

    include_js = cwl.get("requirements").get("InlineJavascriptRequirement")

    assert isinstance(include_js, str)
    assert "engineers walk into a" in include_js


def test_import():
    pass
