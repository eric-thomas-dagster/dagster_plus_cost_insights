# Publishing Guide

This guide will help you publish the dagster_plus_cost_insights package to GitHub.

## 📋 What's Been Prepared

The repository has been structured for publication with:

### ✅ Core Package
- `dagster_insights/` - Main Python package with 18+ integrations
- `pyproject.toml` - Package configuration with dependencies
- `LICENSE` - Apache 2.0 license

### ✅ Documentation
- `README_REPO.md` - Publication-ready README (will replace README.md)
- `CONTRIBUTING.md` - Contribution guidelines
- `docs/` - Additional documentation
- `examples/` - Usage examples

### ✅ Configuration Files
- `.gitignore` - Properly configured to exclude:
  - Reference `dagster-cloud/` directory (4.3MB, not needed)
  - Python artifacts (`__pycache__`, `*.pyc`, etc.)
  - Virtual environments
  - Development docs

### ✅ Helper Script
- `publish.sh` - Interactive script to publish to GitHub

## 🚀 Quick Publish

### Option 1: Use the Helper Script (Recommended)

```bash
cd /Users/ericthomas/Downloads/cost_insights
./publish.sh
```

The script will:
1. Initialize git repository (if needed)
2. Replace README with publication version
3. Stage all files
4. Create commit
5. Add GitHub remote
6. Push to main branch

### Option 2: Manual Steps

```bash
cd /Users/ericthomas/Downloads/cost_insights

# 1. Initialize git
git init

# 2. Replace README
mv README.md docs/ORIGINAL_README.md
mv README_REPO.md README.md

# 3. Stage files
git add .

# 4. Create commit
git commit -m "Initial commit: Dagster+ Cost Insights package"

# 5. Add remote
git remote add origin https://github.com/eric-thomas-dagster/dagster_plus_cost_insights.git

# 6. Push to GitHub
git branch -M main
git push -u origin main
```

## 📦 What Gets Published

### Included (tracked by git)
- ✅ `dagster_insights/` package (69 Python files)
- ✅ `examples/` with usage examples
- ✅ `docs/` with additional documentation
- ✅ `README.md`, `LICENSE`, `CONTRIBUTING.md`
- ✅ `pyproject.toml` for pip installation

### Excluded (in .gitignore)
- ❌ `dagster-cloud/` reference directory (4.3MB)
- ❌ `__pycache__/`, `*.pyc` Python artifacts
- ❌ Virtual environments
- ❌ IDE files (.vscode/, .idea/)
- ❌ Some internal analysis docs

## 🎯 Post-Publication Steps

### 1. Verify on GitHub

Visit: https://github.com/eric-thomas-dagster/dagster_plus_cost_insights

Check:
- README displays correctly
- Examples are visible
- Package structure is clear

### 2. Add GitHub Repository Settings

**Topics/Tags** (for discoverability):
- dagster
- dagster-plus
- cost-insights
- databricks
- data-engineering

**Description**:
> Extended cost insights for Dagster+, supporting Databricks, Redshift, S3, and 15+ more data sources

### 3. Enable GitHub Features

- ✅ Enable **Issues** for bug reports
- ✅ Enable **Discussions** for Q&A
- ✅ Consider adding **Projects** for roadmap

### 4. (Optional) Set Up CI/CD

Create `.github/workflows/test.yml`:

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - run: pip install -e ".[dev]"
      - run: black --check dagster_insights/
      - run: ruff check dagster_insights/
```

## 📢 Announcing the Package

### Internal (Dagster)
- Share in relevant Slack channels
- Update internal documentation
- Add to sales engineering resources

### External (Community)
- Announce in Dagster Slack community
- Share on LinkedIn/Twitter
- Consider blog post about extensibility

## 🔄 Making Updates

After initial publication:

```bash
# Make changes
git add .
git commit -m "Description of changes"
git push
```

### Versioning

Update version in `pyproject.toml`:

```toml
[project]
version = "0.2.0"  # Follow semantic versioning
```

Then:
```bash
git tag v0.2.0
git push --tags
```

## 📦 PyPI Publishing (Future)

To make the package pip-installable:

```bash
# Build package
python -m build

# Upload to PyPI (requires account)
python -m twine upload dist/*
```

Users could then install with:
```bash
pip install dagster-insights
```

## 🆘 Troubleshooting

### "Permission denied" when running publish.sh
```bash
chmod +x publish.sh
```

### Large files in repository
The `.gitignore` should prevent this, but if you see warnings:
```bash
git rm --cached -r dagster-cloud/
```

### Authentication issues
Make sure you're authenticated with GitHub:
```bash
gh auth login
# or use SSH keys
```

## ✅ Checklist

Before publishing, verify:

- [ ] README is clear and accurate
- [ ] Examples work and are well-documented
- [ ] `.gitignore` excludes large/unnecessary files
- [ ] LICENSE is appropriate
- [ ] Dependencies in pyproject.toml are correct
- [ ] No sensitive information (tokens, passwords, etc.)

## 🎉 Ready to Publish!

When you're ready:
```bash
./publish.sh
```

Or use the manual steps above. Good luck! 🚀
