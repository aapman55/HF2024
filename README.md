# HF2024
Hello Fresh assignment

## Setting up environment
For the dependencies Poetry has been used (https://python-poetry.org/). If you have not installed it yet, you 
can pip install the package.

```commandline
pip install poetry
```

Then you can run:
```commandline
poetry install --no-root
```

This way we ensure that the lock file is not computed again and we do not install the python modules from
this project.

## Running the code
The main file is located at `./hf_bi_python_exercise/main.py`.
You can just run:

```commandline
python ./hf_bi_python_exercise/main.py
```

## Running unit tests
You can run the unit tests by:
```commandline
pytest ./tests
```

## Possible issues with spark on Windows
If you are running windows there are a few things you might need to do:
1. Download Hadoop binaries from https://github.com/cdarlint/winutils
2. Add the binaries to the environment variables (as described in the repo above)
3. Windows creates an alias by default for `Python.exe` and `Python3.exe`
    that will point to the windows store. You can disable them in
    `Manage app execution aliases`
4. Pyspark expects the python executable to be called `python3.exe`. Check
   whether this is the case. If not, you can copy the normal `python.exe` and
   name it `python3.exe`.
