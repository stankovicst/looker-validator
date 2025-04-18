# Looker Validator

A command-line tool designed for integrating Looker project validation into CI/CD pipelines. It helps ensure the quality and integrity of your LookML code, content, SQL generation, and data assertions.

## Features

- **LookML Validation**: Checks LookML syntax, references, and adherence to best practices using Looker's native validator.
- **Content Validation**: Validates Looks and Dashboards, checking for errors like broken field references or invalid filters.
  - Optional exclusion of content in personal folders
  - Supports incremental checks comparing branches
- **SQL Validation**: Checks if the SQL generated by Looker for specified explores is valid (by attempting to run queries).
- **Assert Validation (Data Tests)**: Executes LookML data tests defined within your project to verify data integrity rules.
- **Flexible Configuration**: Configure via CLI arguments, environment variables, or a YAML file.
- **CI/CD Friendly**: Designed to be easily integrated into workflows (e.g., GitHub Actions).

## Installation

1. Clone the repository containing this tool.
2. Navigate to the repository's root directory.
3. Install the tool and its dependencies (including the Looker SDK):
   ```bash
   pip install .
   # Or for development:
   # pip install -e .
   ```

## Configuration

The tool requires Looker API credentials and connection details. These can be provided via:
- Command-line Arguments
- Environment Variables
- YAML Configuration File

### Credentials Precedence
1. Command-line arguments
2. Environment variables
3. YAML configuration file
4. Built-in defaults

### Required Credentials
- `LOOKER_BASE_URL` / `--base-url`: Your Looker instance URL (e.g., https://yourcompany.looker.com)
- `LOOKER_CLIENT_ID` / `--client-id`: API Client ID
- `LOOKER_CLIENT_SECRET` / `--client-secret`: API Client Secret

**Recommendation**: Use environment variables or a secrets management system (like GitHub Secrets) for credentials in CI environments.

## Usage

The tool is invoked using the `looker-validator` command followed by a specific validation command and options:

```bash
looker-validator <command> [options]
```

### Commands

#### Connect
Test the connection to the Looker API:
```bash
looker-validator connect --base-url <URL> --client-id <ID> --client-secret <SECRET>
# Or using env vars/config file
looker-validator connect -c looker_validator_config.yaml
```

#### LookML Validation
Runs Looker's native LookML validator:
```bash
looker-validator lookml --project <your-looker-project-name> --branch <your-branch>
# Fail on warnings or errors
looker-validator lookml --project <...> --branch <...> --severity warning
```

#### Content Validation
Validates Looks and Dashboards:
```bash
# Validate content on a specific branch (excluding personal folders)
looker-validator content --project <...> --branch <...>

# Include personal folders
looker-validator content --project <...> --branch <...> --include-personal

# Filter by specific explores or folders
looker-validator content --project <...> --branch <...> --explores 'model_a/explore_one' --folders '123' '-456'

# Incremental check against production branch
looker-validator content --project <...> --branch <feature-branch> --incremental
```

#### SQL Validation
Validates SQL generated for explores:
```bash
looker-validator sql --project <...> --branch <...>
# Validate specific explores with higher concurrency
looker-validator sql --project <...> --branch <...> --explores 'model_a/*' --concurrency 15
```

#### Assert Validation
Runs LookML data tests:
```bash
looker-validator assert --project <...> --branch <...>
# Run tests only for specific explores
looker-validator assert --project <...> --branch <...> --explores 'model_a/explore_one' 'model_b/*'
```

## Configuration File Example

Create a `looker_validator_config.yaml` file:

```yaml
# Connection details (Alternatively use Env Vars or CLI args)
# base_url: https://yourcompany.looker.com
# client_id: your_client_id_from_yaml
# client_secret: your_client_secret_from_yaml

# Common settings
project: "your_primary_looker_project"
timeout: 720
log_dir: "ci_logs"

# Default validator settings
concurrency: 8

# Content validator specific defaults
content:
  folders: ["1", "2", "-99"]
  include_personal: false
  incremental: false

# LookML validator specific defaults
lookml:
  severity: "warning"

# SQL validator specific defaults
sql:
  concurrency: 5

# Assert validator specific defaults
assert:
  concurrency: 5
```

## Options

Detailed options are available for each command. Use `looker-validator <command> --help` for specific command options.

## CI/CD Integration

This tool is ideal for running checks on pull requests or pushes to your Looker project repository.
