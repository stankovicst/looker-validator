# Security Recommendations

## Credential Management

### Local Development
For local development, use a git-ignored config file:

1. Create a `.gitignore` entry:
```
# Ignore local config with credentials
looker_validator_local.yaml
```

2. Create a template config without actual credentials:
```yaml
# looker_validator_template.yaml
base_url: https://your-looker-instance.com
client_id: YOUR_CLIENT_ID
client_secret: YOUR_CLIENT_SECRET
project: your_project_name
```

3. Use environment variables for CI/CD:
```bash
export LOOKER_BASE_URL="https://your-looker-instance.com"
export LOOKER_CLIENT_ID="your_client_id"
export LOOKER_CLIENT_SECRET="your_client_secret"
export LOOKER_PROJECT="your_project"
```

### GitHub Actions Secrets

Store credentials as GitHub repository secrets:

1. Go to your GitHub repository → Settings → Secrets and variables → Actions
2. Add the following secrets:
   - `LOOKER_BASE_URL`
   - `LOOKER_CLIENT_ID`
   - `LOOKER_CLIENT_SECRET`
   - `LOOKER_PROJECT`

### Service Account Recommendation

Use a dedicated Looker API service account with:
- Minimal required permissions
- Development mode access
- Access to specific models only
- Regular credential rotation