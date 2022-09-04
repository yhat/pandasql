README = f"""\
# mypandas — https://github.com/yrom1/mypandas

```py
{open('test_mypandas.py').read()}
```
```
{open('TEST_MYPANDAS_OUTPUT').read()}
```
"""


if __name__ == "__main__":
    with open("README.md", "w") as f:
        f.write(README)
