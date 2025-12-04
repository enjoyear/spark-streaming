installs
```bash
pip install build
```

commands
```bash
./gradlew :word-count-sc:build

# if in the same directory as the pyproject.toml file
python -m build
# if at the parent folder
python -m build word-count-sc
```

# Run code in this folder
## Option 1: local development without installing
```bash
PYTHONPATH=/Users/chenguo/src/chen/spark-streaming/python/src/python python /Users/chenguo/src/chen/spark-streaming/word-count-sc/src/python/fun.py
```

## Option 2: editable installs
The -e (or --editable) flag installs a package in editable/development mode.
Normal install `pip install ./word-count`:
* Copies your package into site-packages
* Changes to your source code require reinstalling to take effect
Editable install `pip install -e ./word-count`:
* Creates a link from site-packages back to your source directory
* Changes to your source code take effect immediately (no reinstall needed)

Use `pip show my-lib` to tell whether it's an editable install or a normal install
```bash
pip install -e ./python -e ./word-count
python /Users/chenguo/src/chen/spark-streaming/word-count-sc/src/python/fun.py
```
