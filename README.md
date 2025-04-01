# Looker Validator Implementation Guide

This guide walks you through setting up the Looker Validator tool for your CI/CD process. 

## Step 1: Set Up a Dedicated Looker User

First, create a dedicated Looker user and API key with the appropriate permissions:

1. **Create a permission set**:
   - Go to Admin > Roles
   - Create a new permission set named "Validator User"
   - Include these permissions:
     - `access_data`
     - `see_lookml_dashboards`
     - `see_looks`
     - `see_user_dashboards`
     - `develop`
     - `see_lookml`
     - `see_sql`

2. **Create a role**:
   - Go to Admin > Roles
   - Create a new role named "Validator User"
   - Add the "Validator User" permission set
   - Select the model set(s) you want to validate

3. **Create the user**:
   - Go to Admin > Users
   - Create a new user (e.g., "validator@yourcompany.com")
   - Assign the "Validator User" role

4. **Generate API credentials**:
   - Edit the user
   - Go to API3 Keys
   - Click "New API Key"
   - Save the client ID and client secret

## Step 2: Install Looker Validator

You can install the validator in your local environment or in your CI system:

```bash
pip install looker-validator
```

## Step 3: Configure the Validator

Create a configuration file `looker_validator_config.yaml`:

```yaml
# Looker API credentials
base_url: https://your-looker-instance.cloud.looker.com
client_id: YOUR_CLIENT_ID
client_secret: YOUR_CLIENT_SECRET
project: your_looker_project_name

# Optional global settings
log_dir: logs

# SQL validator settings
concurrency: 10
incremental: true

# Content validator settings
exclude_personal: true
incremental: true

# LookML validator settings
severity: warning
```

## Step 4: Test the Connection

Verify that you can connect to your Looker instance:

```bash
looker-validator connect --config-file looker_validator_config.yaml
```

You should see a success message with your Looker version.

## Step 5: Set Up GitHub Action

1. **Add secrets to your GitHub repository**:
   - Go to your repository settings
   - Click on Secrets > Actions
   - Add the following secrets:
     - `LOOKER_BASE_URL`: Your Looker instance URL
     - `LOOKER_CLIENT_ID`: The API client ID
     - `LOOKER_CLIENT_SECRET`: The API client secret

2. **Create workflow file**:
   
   Create a file at `.github/workflows/looker-validation.yml`:

   ```yaml
   name: Looker Validation

   on:
     pull_request:
       branches: [ main, master, dev ]
       paths:
         - '**.view.lkml'
         - '**.model.lkml'
         - '**.explore.lkml'
         - '**.dashboard.lookml'
         - '**manifest.lkml'

   jobs:
     validate:
       name: Validate Looker Project
       runs-on: ubuntu-latest
       
       steps:
         - name: Checkout code
           uses: actions/checkout@v3
           
         - name: Set up Python
           uses: actions/setup-python@v4
           with:
             python-version: '3.9'
             
         - name: Install looker-validator
           run: |
             python -m pip install --upgrade pip
             pip install looker-validator
             
         - name: Test connection
           run: |
             looker-validator connect \
               --base-url ${{ secrets.LOOKER_BASE_URL }} \
               --client-id ${{ secrets.LOOKER_CLIENT_ID }} \
               --client-secret ${{ secrets.LOOKER_CLIENT_SECRET }} \
               --project ${{ github.event.repository.name }}
         
         - name: Run LookML validation
           run: |
             looker-validator lookml \
               --base-url ${{ secrets.LOOKER_BASE_URL }} \
               --client-id ${{ secrets.LOOKER_CLIENT_ID }} \
               --client-secret ${{ secrets.LOOKER_CLIENT_SECRET }} \
               --project ${{ github.event.repository.name }} \
               --branch ${{ github.head_ref }}
         
         - name: Run SQL validation
           run: |
             looker-validator sql \
               --base-url ${{ secrets.LOOKER_BASE_URL }} \
               --client-id ${{ secrets.LOOKER_CLIENT_ID }} \
               --client-secret ${{ secrets.LOOKER_CLIENT_SECRET }} \
               --project ${{ github.event.repository.name }} \
               --branch ${{ github.head_ref }} \
               --incremental
         
         - name: Run Content validation
           run: |
             looker-validator content \
               --base-url ${{ secrets.LOOKER_BASE_URL }} \
               --client-id ${{ secrets.LOOKER_CLIENT_ID }} \
               --client-secret ${{ secrets.LOOKER_CLIENT_SECRET }} \
               --project ${{ github.event.repository.name }} \
               --branch ${{ github.head_ref }} \
               --incremental \
               --exclude-personal
         
         - name: Run Assert validation
           run: |
             looker-validator assert \
               --base-url ${{ secrets.LOOKER_BASE_URL }} \
               --client-id ${{ secrets.LOOKER_CLIENT_ID }} \
               --client-secret ${{ secrets.LOOKER_CLIENT_SECRET }} \
               --project ${{ github.event.repository.name }} \
               --branch ${{ github.head_ref }}
         
         - name: Upload logs
           if: always()
           uses: actions/upload-artifact@v3
           with:
             name: validation-logs
             path: logs/
   ```

## Step 6: Customize Your Validation Process

Depending on your needs, you may want to customize your validation:

### For Large Projects

If you have a large Looker project, consider:

1. **Selecting specific models/explores**:
   ```bash
   looker-validator sql --explores model_a/* model_b/explore_c
   ```

2. **Increasing concurrency**:
   ```bash
   looker-validator sql --concurrency 20
   ```

3. **Running validators in sequence** to avoid branch conflicts

### For Faster Validations

To speed up validation:

1. **Use incremental mode**:
   ```bash
   looker-validator sql --incremental
   ```

2. **Use fail-fast for SQL validation**:
   ```bash
   looker-validator sql --fail-fast
   ```

3. **Exclude specific explores/folders**:
   ```bash
   looker-validator content --explores -model_a/explore_b --folders -33
   ```

## Step 7: Handling Errors

When validation fails, the GitHub Action will fail with a non-zero exit code. The logs will contain detailed information about the failures.

You can view the logs in two ways:

1. **GitHub Actions logs**: View the output directly in the GitHub Actions interface

2. **Downloaded artifacts**: Download the logs artifact for detailed error information

## Best Practices

1. **Ignore dimensions that validly fail**:
   ```lookml
   dimension: complex_calculation {
     sql: ${TABLE}.value ;;
     tags: ["spectacles: ignore"]
   }
   ```

2. **Use incremental validation** to only show new errors

3. **Run all validators** for comprehensive testing

4. **Consider test concurrency** in CI environments

5. **Set up branch protection rules** to prevent merging PRs with validation failures

## Troubleshooting

### Common Issues

1. **Authentication Failures**:
   - Verify API credentials
   - Check API user permissions

2. **Branch Conflicts**:
   - Use separate API users for concurrent validations
   - Add concurrency limits to your GitHub workflow

3. **SQL Errors**:
   - Check the SQL logs for specific error details
   - Ignore dimensions that validly fail

4. **Content Errors**:
   - Use `--exclude-personal` to focus on shared content
   - Check for missing fields referenced by dashboards

### Getting Help

If you encounter issues:

1. Run validators with `--verbose` for detailed logs
2. Check log files in the `logs` directory
3. Review the GitHub Action output