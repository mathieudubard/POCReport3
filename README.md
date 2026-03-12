# Model Report Generator

## Versioning

The single source of truth for this repository versioning will be the model registry.

## Project Structure

```
./
├── bin/                     # (optional scripts)
├── config/                  # config.py, local.ini, logging.ini, model-conf-*.ini
├── datamodel/               # ImpairmentStudio-DataDictionary.csv
├── meta/                    # model.json
├── model/
│   ├── iosession.py         # S3 and local file I/O
│   ├── model.py             # Model logic and quarterly summary report
│   └── run.py               # Program entry
├── sample/                  # useCase.txt (report instructions)
├── tests/
│   └── test_model.py        # Unit tests (pytest)
├── requirements.txt
└── README.md
```

## Prerequisites

- Python 3.x
- `pip install -r requirements.txt` (pandas, pyarrow, pytest)
- For S3 runs: `moodyscappy` (Cappy) and credentials (JWT or username/password)

## Running the model

S3 (default):

```bash
python ./model/run.py -j <jwt_token> -s <s3-key-to-modelRunParameters.json>
```

Local test folder:

```bash
python ./model/run.py -L <test_folder_path> -j <jwt_token>
```

## Running tests

From the project root:

```bash
python -m pytest tests/ -v
```

## Script arguments

```bash
python ./model/run.py -h
```

Required: one of `-s` (S3) or `-L` (local), and one of `-j` (JWT) or `-u` (username password). Optional: `-l` loglevel, `-k` keeptemp, `-o`/`-c` config, `-t`/`-p` proxy.
