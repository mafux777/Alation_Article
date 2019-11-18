import requests
import json
import sys
from collections import deque
import re
from time import sleep


test = {
  "folders": [{
    "name": "Folder 1",
    "collections": [{
      "name": "Collection 0",
      "schemata": [{   # schemata is a list of dicts, each schema with a name and a definition
        "name":"Schema 0",
        "definition": { # the definition is a dict with title, type, and *properties* (for objects) or *items* (for arrays)
          "title": "Title 0", # this seems to be ignored
          "type": "Object", # this does not seem to make much sense
          "required": ["$attribute1", "$attribute2"],
          "properties": { # properties is a dict with one key per attribute
            "$attribute1": {
              "type": "type_of_1" # each attribute is a dict with a single key: type
            },
            "$attribute2": {
              "type": "type_of_2"
            }
          }
        }
      }]
    }]
  }]
}

#url2 = "https://funkmeister510.alationcatalog.com/integration/v1/data/6/parse_docstore/"
url = "http://18.218.6.215/"
api = "integration/v1/data/3/parse_docstore/"
headers = dict(token='e3a43bb9-b62c-48c8-9bea-6feccc265356')

dexcom = {'folders':[{'name':'Root Folder',
                      'collections':[]}]}

list_of_collections = dexcom['folders'][0]['collections']
dict_of_types = {}

# Let's create a new collection for each key space
def create_keyspace(line, source):
  # match key space to folder
  words = line.split()
  list_of_collections.append({'name':words[2], 'schemata':[]})

  return list_of_collections[-1]['schemata'] # this is a list of schemas

# Let's create a new type and cache them
def create_type(line, source):
  words = line.split()
  type_name = words[2].split('.')[1]
  dict_of_attributes = {}
  while True:
      line_1 = source.popleft()
      if not line_1:
        continue
      if line_1==');':
        break
      words_1 = line_1.split()
      name_of_attrib = words_1[0]
      type_of_attrib = words_1[1].rstrip(',')
      m = re.search(r'frozen<([a-z_]+)>', type_of_attrib)
      if m:
        complex_type = m.group(1)
        if complex_type in dict_of_types:
          type_of_attrib=dict_of_types[complex_type]
          dict_of_attributes[name_of_attrib] = type_of_attrib
          #print(type_of_attrib)
        else:
          print("No complex type for {}".format(complex_type))
      else:
        dict_of_attributes[name_of_attrib] = {'type': type_of_attrib}
  # the purpose of this method is to build a list of existing types
  print("Added <{}> as a complex type.".format(type_name))
  dict_of_types[type_name]={'type':'object','properties':dict_of_attributes}

# Let's create a table as a schema in a collection
def create_table(line, source):
  # create the empty table first
  words = line.split()
  table_name = words[2]
  # initialize a dict of attributes of this table
  dict_of_attributes = {}
  # go and look for attributes
  while True:
      line_1 = source.popleft()
      if not line_1:
        continue # empty line -> try again
      if line_1[0]==')': # end of attribute definitions!
        # Let's now destroy the rest of the table def
        while True:
          line_2 = source.popleft()
          if not line_2:
            continue
          if line_2[-1] ==';': # until we find a semicolon
            break
        break
      # Apart from lines with primary key(...) :
      if not re.search(r'PRIMARY KEY[ ]?\(', line_1):
        words_1 = line_1.split()
        name_of_attrib = words_1[0]
        type_of_attrib = words_1[1].rstrip(',')
        # look for a complex type
        m = re.search(r'frozen<([a-z_]+)>', type_of_attrib)
        if m:
          complex_type = m.group(1)
          # It should have been defined before, and therefore cached
          if complex_type in dict_of_types:
            type_of_attrib=dict_of_types.copy()[complex_type]
            dict_of_attributes[name_of_attrib] = type_of_attrib
          else:
            print("No complex type for {}".format(complex_type))
        else:
          # Just a simple attribute
          dict_of_attributes[name_of_attrib] = {'type': type_of_attrib}

  print("Created table {}:{}".format(table_name, dict_of_attributes))
  return {'name':table_name, 'definition':{'type':'object',
    'properties':dict_of_attributes}}


args = sys.argv
with open(args[1]) as f:
    source = deque(f.read().splitlines())


while True:
  if len(source)==0:
    break
  line = source.popleft()
  if not line:
    continue
  components = line.split()
  if len(components)<=2:
    continue
  command = components[0] + " " + components[1]
  if command == 'CREATE KEYSPACE':
    list_of_schemas = create_keyspace(line, source)
      # we use this later to append any tables we find
  if command == 'CREATE TYPE':
    create_type(line, source)
  if command == 'CREATE TABLE':
     schema = create_table(line, source)
     # Now insert this at the right place in the structure
     list_of_schemas.append(schema)

params=dict(remove_not_seen=False)
r = requests.post(url+api, json=dexcom, headers=headers, params=params)
job_id = json.loads(r.content)['job_id']

sleep(2)

url2 = url + 'api/v1/bulk_metadata/job/'
params = dict(id=job_id)
r2 = requests.get(url2, params=params, headers=headers)

r3 = json.loads(r2.content)
print('--------------')
print(r3['status'])
print(r3['msg'])
print(r3['result'])






