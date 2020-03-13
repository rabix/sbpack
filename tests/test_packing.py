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

    include_js = cwl.get("requirements").get("InlineJavascriptRequirement").get("expressionLib")

    assert isinstance(include_js, list)
    assert "engineers walk into a" in include_js[0]


def test_schema_def1():
    cwl = pack("remote-cwl/tool2.cwl")
    _type = cwl.get("inputs").get("in1").get("type")
    assert isinstance(_type, dict)
    assert _type.get("type") == "array"


def test_schema_def2():
    cwl = pack("wf2.cwl")
    _type = cwl.get("inputs").get("in2").get("type")
    assert isinstance(_type, dict)
    assert _type.get("type") == "enum"


def test_step_packing():
    cwl = pack("remote-cwl/wf1.cwl")
    s1 = cwl.get("steps").get("s1")
    tool2 = s1.get("run")
    _type = tool2.get("inputs").get("in1").get("type")
    assert isinstance(_type, dict)
    assert _type.get("type") == "array"


def test_remote_packing():
    cwl = pack("https://raw.githubusercontent.com/kaushik-work/sbpack/master/tests/wf2.cwl")
    s1 = cwl.get("steps").get("s1")
    wf1 = s1.get("run")
    assert wf1.get("class") == "Workflow"

    tool2 = wf1.get("steps").get("s1").get("run")
    _type = tool2.get("inputs").get("in1").get("type")
    assert isinstance(_type, dict)
    assert _type.get("type") == "array"
