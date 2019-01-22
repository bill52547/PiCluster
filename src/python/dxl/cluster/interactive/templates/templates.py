from jinja2 import Environment, FileSystemLoader, select_autoescape

env = Environment(
    loader=FileSystemLoader('.'),
    autoescape=select_autoescape(['yml', "j2"])
)

query_update = """
mutation {
  update_{{table_name}}(
    where: {id: {_eq: {{id}}}},
    _set: {
      {% for k,v in patches.items() %}{{k}}: "{{v}}"{% endfor %}
    }
  ){
    returning{
      id
      state
    }
  }
}
"""

query_read = """
query {
  {{table_name}}(
    where: { {{item}}: {_eq: "{{condition}}"}}
  ){
    {% for i in returns %}{{i}}\n{% endfor %}
  }
}
"""


query_insert = """
mutation {
  insert_{{table_name}}(
    objects:[
      {
        {% for k,v in inserts.items() %}{{k}}: "{{v}}"{% endfor %}
      }
    ]
  ){
    returning {
      id
    }
  }
}
"""


master_task_config = """
version: v0.0.1
kind: masterTaskConfiguration
metadata:
  backend: {{backend}}
  workdir: {{workdir}}
  mastTaskID: {{mastTaskID}}
spec:
  init:
   nb_split: {{nb_split}}
  inputs:
    ioCollections:
      comments: monteCarloSimu
      returns:
      - url
    macs:
      comments: {{mac}} 
      returns:
      - url
    phantomHeaders:
      comments: {{phantom_header}}
      returns:
      - url
    phantoms:
      id: {{phantom_id}}
      returns:
      - phantom_bin
      - activity_range
      - range_material
  outputs:
  - result.root
  procedures:
  - 
    ["pygate","init","subdir","-n","{{nb_split}}"]
  - 
    ["pygate","init","bcast"]
  - 
    ["pygate","submit"]
"""
