import base64
import re
import json


def combine_lines(lines):
    new_lines = []
    for line in lines:
        if line.startswith("."):
            new_lines[-1] += line
        else:
            new_lines.append(line)

    return new_lines


def deduplicate(files):
    new_files = []
    for file in files:
        has_duplicate = len([x for x in new_files if x['file_or_folder'] ==
                             file['file_or_folder'] and x['container'] == file['container']]) > 0
        if not has_duplicate:
            new_files.append(file)
    return new_files


def lines(s):
    return combine_lines([line.strip() for line in s.split("\n")])


def is_input_line(l):
    return "spark.read." in l


def is_output_line(l):
    return ".write." in l


regex = r"abfss:\/\/([a-zA-Z-]+)@([a-z]+)\.dfs\.core\.windows\.net\/([a-zA-Z\._-]+)"

notebooks = [{"name": "notebook.scala"}]

files = []
for notebook in notebooks:
    with open(notebook['name'], 'r')as f:
        contents = base64.b64encode(f.read().encode())

    code_lines = lines(base64.b64decode(contents).decode())
    for line in code_lines:
        res = re.search(regex, line)
        if res:
            if len(res.groups()) == 3:
                container, account, file_path = res.groups()
                files.append({
                    "container": container,
                    "file_or_folder": file_path,
                    "is_input": is_input_line(line),
                    "is_output": is_output_line(line)
                })
            elif len(res.groups()) == 6:
                icontainer, iaccount, ifile_path = res.groups()[:3]
                ocontainer, oaccount, ofile_path = res.groups()[4:]
                files.append({
                    "container": icontainer,
                    "file_or_folder": ifile_path,
                    "is_input": True,
                    "is_output": False
                })
                files.append({
                    "container": ocontainer,
                    "file_or_folder": ofile_path,
                    "is_input": False,
                    "is_output": True
                })

files = deduplicate(files)

inventory = {}
for file in files:
    container = file['container']
    if inventory.get(container) is None:
        inventory[container] = {}

        if file['is_input']:
            inventory[container]['inputs'] = [file]
        if file['is_output']:
            inventory[container]['outputs'] = [file]
    else:
        inventory_container = inventory.get(container)

        if file['is_input']:
            inventory_container.get('inputs').append(file)
        if file['is_output']:
            inventory_container.get('outputs').append(file)

print(json.dumps(inventory, indent=2))
