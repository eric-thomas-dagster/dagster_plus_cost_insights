# Publishing to PyPI

This guide explains how to publish `dagster-insights` to PyPI so developers can install it easily.

## One-Time Setup

### 1. Create PyPI Account
- Go to https://pypi.org/account/register/
- Create an account

### 2. Create API Token
- Go to https://pypi.org/manage/account/token/
- Create a new API token with scope for this project
- **Save the token** - you'll only see it once!

### 3. Add Token to GitHub Secrets
- Go to your GitHub repo: https://github.com/eric-thomas-dagster/dagster_plus_cost_insights
- Go to Settings → Secrets and variables → Actions
- Click "New repository secret"
- Name: `PYPI_API_TOKEN`
- Value: Your PyPI token (starts with `pypi-`)

## Publishing a New Version

### Method 1: Using GitHub Releases (Recommended - Automatic)

1. **Update version** in `pyproject.toml`:
   ```toml
   version = "0.1.1"  # Increment version
   ```

2. **Commit and push** changes:
   ```bash
   git add pyproject.toml
   git commit -m "Bump version to 0.1.1"
   git push
   ```

3. **Create a GitHub Release**:
   - Go to: https://github.com/eric-thomas-dagster/dagster_plus_cost_insights/releases/new
   - Click "Choose a tag" and create new tag: `v0.1.1`
   - Title: `v0.1.1`
   - Description: Add release notes (what changed)
   - Click "Publish release"

4. **GitHub Actions will automatically**:
   - Build the package
   - Publish to PyPI
   - Available in ~5 minutes!

### Method 2: Manual Publishing (If needed)

If you need to publish manually:

```bash
# Install build tools
pip install build twine

# Build the package
python -m build

# Upload to PyPI (will prompt for credentials)
twine upload dist/*
```

## After Publishing

Developers can now install with:

```bash
# Basic installation
pip install dagster-insights

# With specific integrations
pip install dagster-insights[databricks]
pip install dagster-insights[redshift,dbt]
pip install dagster-insights[all]

# With uv
uv add dagster-insights[databricks]
```

## Version Numbering

Follow semantic versioning (semver):
- **Major** (1.0.0): Breaking changes
- **Minor** (0.1.0): New features, backward compatible
- **Patch** (0.0.1): Bug fixes

Examples:
- `0.1.0` → `0.1.1`: Bug fix
- `0.1.0` → `0.2.0`: New feature (added dbt support)
- `0.9.0` → `1.0.0`: Stable release or breaking change

## First-Time Publishing Checklist

- [ ] PyPI account created
- [ ] API token generated
- [ ] Token added to GitHub secrets
- [ ] Version set in pyproject.toml
- [ ] README.md updated with installation instructions
- [ ] Create first release on GitHub
- [ ] Verify package appears on PyPI
- [ ] Test installation: `pip install dagster-insights`
