from sbpack.pack import pack


def _find(l: list, key: str, val: str):
    return next(_x for _x in l if _x[key] == val)


def test_port_normalization():
    cwl = pack("remote-cwl/wf1.cwl")
    step_s1 = _find(cwl.get("steps"), "id", "s1")
    step_in1 = _find(step_s1.get("in"), "id", "in1")
    assert step_in1["source"] == "in1"

    cwl = pack("wf2.cwl")
    step_s1 = _find(cwl.get("steps"), "id", "s1")
    step_in1 = _find(step_s1.get("in"), "id", "in1")
    assert step_in1["source"] == "in1"

    out1 = _find(cwl.get("outputs"), "id", "out1")
    assert out1.get("outputSource") == "s2/out1"


def test_include():
    cwl = pack("remote-cwl/tool1.cwl")
    assert "arguments" in cwl
    assert isinstance(cwl.get("arguments"), list)

    inline_js_req = _find(
        cwl.get("requirements"),
        "class",
        "InlineJavascriptRequirement"
    )
    include_js = inline_js_req.get("expressionLib")

    assert isinstance(include_js, list)
    assert "engineers walk into a" in include_js[0]


def test_schema_def1():
    cwl = pack("remote-cwl/tool2.cwl")
    _type = _find(cwl.get("inputs"), "id", "in1").get("type")
    assert isinstance(_type, dict)
    assert _type.get("type") == "array"


def test_schema_def2():
    cwl = pack("wf2.cwl")
    _type = _find(cwl.get("inputs"), "id", "in2").get("type")
    assert isinstance(_type, dict)
    assert _type.get("type") == "enum"


def test_step_packing():
    cwl = pack("remote-cwl/wf1.cwl")
    s1 = _find(cwl.get("steps"), "id", "s1")
    tool2 = s1.get("run")
    _type = _find(tool2.get("inputs"), "id", "in1").get("type")
    assert isinstance(_type, dict)
    assert _type.get("type") == "array"


def test_embedded_packing():
    cwl = pack("workflows/count-lines16-wf.cwl")


def test_embedded_packing_with_ids():
    cwl = pack("workflows/count-lines16-wf.cwl", add_ids=True)
    assert (
            cwl["steps"][0]["run"]["id"]
            == "count-lines16-wf.cwl@step_step1@run"
    )
    assert (
        cwl["steps"][0]["run"]["steps"][0]["run"]["id"]
        == "count-lines16-wf.cwl@step_step1@wc-tool.cwl"
    )
    assert (
        cwl["steps"][0]["run"]["steps"][1]["run"]["steps"][0]["run"]["id"]
        == "count-lines16-wf.cwl@step_step1@parseInt-tool.cwl"
    )


def test_step_process_id():
    """Workflow step with external "run" reference gets an "id" added."""
    cwl = pack("workflows/wf2.cwl", add_ids=True)
    assert cwl["id"] == "wf2.cwl"
    s1 = _find(cwl.get("steps"), "id", "s1")
    assert s1["run"]["id"] == "wf2.cwl@step_s1@clt2.cwl"


def test_remote_packing():
    cwl = pack(
        "https://raw.githubusercontent.com/kaushik-work/"
        "sbpack/master/tests/wf2.cwl"
    )
    s1 = _find(cwl.get("steps"), "id", "s1")
    wf1 = s1.get("run")
    assert wf1.get("class") == "Workflow"

    tool2 = _find(wf1.get("steps"), "id", "s1").get("run")
    _type = _find(tool2.get("inputs"), "id", "in1").get("type")
    assert isinstance(_type, dict)
    assert _type.get("type") == "array"


def test_remote_packing_github_soft_links():
    cwl = pack(
        "https://raw.githubusercontent.com/rabix/"
        "sbpack/master/tests/workflows/wf5.cwl"
    )
    s1 = _find(cwl.get("steps"), "id", "s1")
    tool1 = s1.get("run")
    assert tool1.get("class") == "CommandLineTool"


def test_already_packed_graph():
    """Workflow already packed in a $graph."""
    cwl = pack("workflows/scatter-wf4.cwl")
    assert "inputs" not in cwl
    assert "outputs" not in cwl
    assert "$graph" in cwl
    assert "requirements" not in cwl


def test_import_in_type():
    cwl = pack("workflows/import-in-type.cwl")
    assert cwl["inputs"][0]["type"] == ["File", "Directory"]
