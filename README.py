from subprocess import run


def _run(cmd: str) -> None:
    run(cmd, shell=True)


_run("touch TEST_MYPANDAS_OUTPUT")
_run("py ./tests/test_mypandas.py > TEST_MYPANDAS_OUTPUT")

README = f"""\
# [mypandas](https://github.com/yrom1/mypandas) — MySQL for Pandas

A package that lets you query pandas DataFrames with MySQL!

## Notice
This is a work in progress!

## Install

Currently available on [PyPI](https://pypi.org/project/mypandas/), to install:
```
pip install mypandas
```

## Example

```py
{''.join(open('./tests/test_mypandas.py').readlines()[4:])}
```
```
{open('TEST_MYPANDAS_OUTPUT').read()}
```
"""

_run("rm TEST_MYPANDAS_OUTPUT")


if __name__ == "__main__":
    with open("README.md", "w") as f:
        f.write(README)
