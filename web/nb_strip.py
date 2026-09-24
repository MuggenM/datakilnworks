"""Git clean filter for notebooks: prints the notebook on stdin without outputs and execution counts.

Outputs can hold query results the author was allowed to see (masked or row-filtered data, tokens printed by accident), and they
are noise in diffs. The filter only shapes what is committed; the notebook in the working tree is never modified.
Anything that is not a notebook JSON passes through unchanged, so a broken file is never lost by the filter.
"""
import json
import sys


def strip(text: str) -> str:
    try:
        nb = json.loads(text)
        cells = nb["cells"]
    except Exception:
        return text
    for cell in cells:
        if cell.get("cell_type") == "code":
            cell["outputs"] = []
            cell["execution_count"] = None
    return json.dumps(nb, indent=1, ensure_ascii=False) + "\n"


if __name__ == "__main__":
    sys.stdout.write(strip(sys.stdin.read()))
