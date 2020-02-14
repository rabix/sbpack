from sbpack.pack import pack


def test_include():
    cwl = pack("remote-cwl/tool1.cwl")
    assert "arguments" in cwl
    assert isinstance(cwl.get("arguments"), list)

    include_js = cwl.get("requirements").get("InlineJavascriptRequirement")

    assert isinstance(include_js, str)
    assert "engineers walk into a" in include_js
