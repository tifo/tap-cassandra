# tap-cassandra

`tap-cassandra` is a Singer tap for Cassandra.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

<!--

Developer TODO: Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

Install from PyPi:

```bash
pipx install tap-cassandra
```

-->

## Installation

Install from GitHub:

```bash
pipx install git+https://github.com/datarts-tech/tap-cassandra.git@main
```



## Capabilities

* `catalog`
* `state`
* `discover`
* `about`
* `stream-maps`
* `schema-flattening`
* `batch`

## Settings

| Setting             | Required | Default | Description |
|:--------------------|:--------:|:-------:|:------------|
| hosts               | True     | None    | The list of contact points to try connecting for cluster discovery. |
| port                | False    |    9042 | The server-side port to open connections to. Defaults to 9042.. |
| keyspace            | True     | None    | Keyspace will be the default keyspace for operations on the Session. |
| username            | True     | None    | The username passed as a PlainTextAuthProvider username. |
| password            | True     | None    | The password passed as a PlainTextAuthProvider password. |
| start_date          | False    | None    | The earliest record date to sync. |
| request_timeout     | False    |     100 | Request timeout used when not overridden in Session.execute(). |
| local_dc            | False    | None    | The local_dc parameter should be the name of the datacenter. |
| reconnect_delay     | False    |      60 | Floating point number of seconds to wait inbetween each attempt. |
| max_attempts        | False    |       5 | Should be a total number of attempts to be made before giving up. |
| protocol_version    | False    |      65 | The maximum version of the native protocol to use. |
| stream_maps         | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config   | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled  | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth| False    | None    | The max depth to flatten schemas. |
| batch_config        | False    | None    |             |

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-cassandra --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

<!--
Developer TODO: If your tap requires special access on the source system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `tap-cassandra` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-cassandra --version
tap-cassandra --help
tap-cassandra --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-cassandra` CLI interface directly using `poetry run`:

```bash
poetry run tap-cassandra --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-cassandra
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-cassandra --version
# OR run a test `elt` pipeline:
meltano elt tap-cassandra target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
