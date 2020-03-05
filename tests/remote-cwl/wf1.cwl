class: Workflow
cwlVersion: v1.0
inputs:
  in1: testtypes.yml#my_boolean

steps:
  s1:
    run: ./tool2.cwl
    in:
      in1: "#in1"   # This should be normalized out
    out: [out1]
  s2:
    run: tool1.cwl
    in:
      in1: s1/out1
    out: [out1]

outputs:
  - id: out1 
    type: string 
    outputSource: "#s2/out1"

requirements:
  SchemaDefRequirement:
    types:
      - $import: testtypes.yml