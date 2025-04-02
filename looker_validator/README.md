# Looker Validator Documentation

## Overview

Looker Validator is a continuous integration tool for Looker and LookML validation. It performs comprehensive validation checks against your Looker instance to catch errors before deploying to production.

## Installation

```bash
pip install looker-validator
```

Or from source:

```bash
git clone https://github.com/stankovicst/looker-validator.git
cd looker-validator
pip install -e .
```

## Configuration

Looker Validator supports three configuration methods (in order of precedence):

1. Command line parameters
2. Environment variables
3. YAML configuration file

### YAML Configuration

Create a `looker_validator_config.yaml` file:

```yaml
# Looker API credentials
base_url: https://company.looker.com
client_id: your_client_id
client_secret: your_client_secret
project: your_project_name

# Optional settings
api_version: 4.0  # Always use 4.0 with SDK version 24.16.2
timeout: 600      # 10 minutes

# SQL validator settings
concurrency: 10
incremental: true

# Content validator settings
exclude_personal: true
```

### Environment Variables

```bash
export LOOKER_BASE_URL="https://company.looker.com"
export LOOKER_CLIENT_ID="your_client_id"
export LOOKER_CLIENT_SECRET="your_client_secret"
export LOOKER_PROJECT="your_project_name"
```

## Usage

### Test Connection

```bash
looker-validator connect
```

### Validate LookML Syntax

```bash
looker-validator lookml --severity warning --branch feature_branch
```

### Validate SQL

```bash
looker-validator sql --branch feature_branch --incremental
```

### Run Looker Data Tests

```bash
looker-validator assert --branch feature_branch
```

### Validate Content

```bash
looker-validator content --branch feature_branch --exclude-personal
```

## Command Reference

### Global Options

| Option | Environment Variable | Description |
|--------|---------------------|-------------|
| `--config-file`, `-c` | | Path to YAML config file |
| `--base-url` | `LOOKER_BASE_URL` | Looker instance URL |
| `--client-id` | `LOOKER_CLIENT_ID` | Looker API client ID |
| `--client-secret` | `LOOKER_CLIENT_SECRET` | Looker API client secret |
| `--port` | `LOOKER_PORT` | Looker API port |
| `--api-version` | `LOOKER_API_VERSION` | Looker API version (default: 4.0) |
| `--project` | `LOOKER_PROJECT` | Looker project name |
| `--branch` | `LOOKER_GIT_BRANCH` | Git branch name (default: production) |
| `--commit-ref` | `LOOKER_COMMIT_REF` | Git commit reference |
| `--remote-reset` | `LOOKER_REMOTE_RESET` | Reset branch to remote state |
| `--log-dir` | `LOOKER_LOG_DIR` | Directory for log files |
| `--verbose`, `-v` | `LOOKER_VERBOSE` | Enable verbose logging |
| `--pin-imports` | `LOOKER_PIN_IMPORTS` | Pin imported projects (format: 'project:ref,project2:ref2') |
| `--timeout` | `LOOKER_TIMEOUT` | API request timeout in seconds (default: 600) |

### SQL Validator Options

| Option | Environment Variable | Description |
|--------|---------------------|-------------|
| `--explores` | `LOOKER_EXPLORES` | Model/explore selectors (e.g. 'model_a/*', '-model_b/explore_c') |
| `--concurrency` | `LOOKER_CONCURRENCY` | Number of concurrent queries (default: 10) |
| `--fail-fast` | `LOOKER_FAIL_FAST` | Only run explore-level queries |
| `--profile`, `-p` | `LOOKER_PROFILE` | Profile query execution time |
| `--runtime-threshold` | `LOOKER_RUNTIME_THRESHOLD` | Runtime threshold for profiler in seconds (default: 5) |
| `--incremental` | `LOOKER_INCREMENTAL` | Only show errors unique to the branch |
| `--target` | `LOOKER_TARGET` | Target branch for incremental comparison (default: production) |
| `--ignore-hidden` | `LOOKER_IGNORE_HIDDEN` | Ignore hidden dimensions |
| `--chunk-size` | `LOOKER_CHUNK_SIZE` | Maximum dimensions per query (default: 500) |

### LookML Validator Options

| Option | Environment Variable | Description |
|--------|---------------------|-------------|
| `--severity` | `LOOKER_SEVERITY` | Severity threshold (info, warning, error) |

### Content Validator Options

| Option | Environment Variable | Description |
|--------|---------------------|-------------|
| `--explores` | `LOOKER_EXPLORES` | Model/explore selectors |
| `--folders` | `LOOKER_FOLDERS` | Folder IDs to include/exclude (e.g. '25', '-33') |
| `--exclude-personal` | `LOOKER_EXCLUDE_PERSONAL` | Exclude content in personal folders |
| `--incremental` | `LOOKER_INCREMENTAL` | Only show errors unique to the branch |
| `--target` | `LOOKER_TARGET` | Target branch for incremental comparison |

### Assert Validator Options

| Option | Environment Variable | Description |
|--------|---------------------|-------------|
| `--explores` | `LOOKER_EXPLORES` | Model/explore selectors |

## Explore Selectors

Use explore selectors to filter which explores are validated:

- Include all explores in a model: `model_name/*`
- Include a specific explore: `model_name/explore_name`
- Exclude a specific explore: `-model_name/explore_name`
- Include all explores with a specific name: `*/explore_name`

Examples:

```bash
# Validate all explores in 'marketing' model
looker-validator sql --explores "marketing/*"

# Validate all explores except those in 'deprecated' model
looker-validator sql --explores "*/common" --explores "-deprecated/*"
```

## Import Pinning

Pin imported projects to specific branches or commits:

```bash
looker-validator sql --pin-imports "shared_models:main,common:feature_branch"
```

## CI/CD Integration

### GitHub Actions

Create a `.github/workflows/looker-validation.yml` file in your repository:

```yaml
name: Looker Validation

on:
  pull_request:
    branches: [ main, master ]
    paths:
      - '**/*.lkml'

jobs:
  validate:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.9'
    
    - name: Install dependencies
      run: |
        pip install looker-validator
    
    - name: Validate Looker project
      run: |
        looker-validator sql --branch ${{ github.head_ref }} --incremental
      env:
        LOOKER_BASE_URL: ${{ secrets.LOOKER_BASE_URL }}
        LOOKER_CLIENT_ID: ${{ secrets.LOOKER_CLIENT_ID }}
        LOOKER_CLIENT_SECRET: ${{ secrets.LOOKER_CLIENT_SECRET }}
        LOOKER_PROJECT: ${{ secrets.LOOKER_PROJECT }}
```

## Troubleshooting

### Common Issues

#### Timeout Errors

If you encounter timeout errors with large projects:

1. Increase the timeout:
   ```bash
   looker-validator sql --timeout 1200  # 20 minutes
   ```

2. Reduce concurrency:
   ```bash
   looker-validator sql --concurrency 5
   ```

3. Reduce chunk size:
   ```bash
   looker-validator sql --chunk-size 200
   ```

#### SDK Compatibility

This tool is specifically designed for Looker SDK 24.16.2 which only supports API 4.0.

#### API Rate Limiting

If you encounter rate limiting:

1. Reduce concurrency
2. Add delay between operations:
   ```python
   import time
   time.sleep(1)  # 1 second delay
   ```

## Advanced Usage

### Custom Branch Management

For complex CI workflows, you can pin imports to specific references:

```bash
looker-validator sql \
  --branch feature_branch \
  --pin-imports "shared:main,common:refs/heads/dev" \
  --remote-reset
```

### Using with Pull Requests

To compare changes from a pull request:

```bash
looker-validator sql \
  --branch ${PR_BRANCH} \
  --incremental \
  --target ${BASE_BRANCH}
```