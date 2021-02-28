import requests
import json
import base64
import re
from pprint import pprint

config = {
  "base_url": "...",
  "token": "...",
  "mount_map": {
    
  }
}

def lines(s):
    return [line for line in s.split("\n")]

def flatten(l):
    if l == []:
        return l
    if isinstance(l[0], list):
        return flatten(l[0]) + flatten(l[1:])
    return l[:1] + flatten(l[1:])

def databricks_request(endpoint, params, method = "GET"):
  token = config['token']
  headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
  }
  

  if method == "GET":
    return ( 
      requests
        .get(
          config['base_url'] + endpoint, 
          params=params,
          headers=headers)
        .json())

  elif method == "POST":
    pprint(params)
    return (
      requests
        .post(
          config['base_url'] + endpoint,
          data=json.dumps(params),
          headers=headers
        )
        .json())

  else:
    raise Exception("Unsupported request method")

def map_notebook(n):
    if n["object_type"] == "DIRECTORY":
        return list(map(lambda x: map_notebook(x), databricks_request("workspace/list", {"path": n["path"]}).get("objects", [])))
    else:
        return n


def get_all_notebooks():
    root = "/"

    return flatten(list(map(lambda x: map_notebook(x), databricks_request("workspace/list", {"path": root}).get("objects", []))))

def upload_notebook(notebook, contents):
  params = {
    "content": base64.b64encode(contents.encode()).decode(),
    "path": notebook["path"],
    "language": notebook["language"],
    "overwrite": True,
    "format": "SOURCE"
  }
  return databricks_request("workspace/import", params, "POST")

path_regex = r"abfss:\/\/([a-zA-Z-]+)@([a-z]+)\.dfs\.core\.windows\.net\/([a-zA-Z\._-]+)"

notebooks = get_all_notebooks() 

files = []
for notebook in notebooks:
  contents = lines(base64.b64decode(databricks_request("workspace/export", {"path": notebook["path"], "format": "SOURCE"}).get("content", "")).decode())

  new_lines = []
  for line in contents:
    res = re.search(path_regex, line)
    
    if res:
      container, account, path = res.groups()
      mount_point = config.get("mount_map", {}).get(container, "")

      if mount_point.endswith("/"):
        mount_point = mount_point[:-1]

      if mount_point == "":
        raise Exception(f"No configuration for container: {container}")

      new_path = f"{mount_point}/{path}"
      new_line = re.sub(path_regex, new_path, line)
      new_lines.append(new_line)

    else:
      new_lines.append(line)

  new_contents = "\n".join(new_lines)
  res = upload_notebook(notebook, new_contents)