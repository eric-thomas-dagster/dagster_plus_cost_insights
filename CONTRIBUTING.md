# Contributing to Dagster+ Cost Insights

Thank you for your interest in contributing! This guide will help you get started.

## Development Setup

### 1. Clone the Repository

```bash
git clone https://github.com/eric-thomas-dagster/dagster_plus_cost_insights.git
cd dagster_plus_cost_insights
```

### 2. Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Development Dependencies

```bash
pip install -e ".[dev,all]"
```

## Project Structure

```
dagster_plus_cost_insights/
├── dagster_insights/          # Main package
│   ├── __init__.py
│   ├── insights_utils.py
│   ├── databricks/           # Databricks integration
│   ├── redshift/             # Redshift integration
│   ├── storage/              # Storage integrations (S3, GCS, Azure)
│   ├── aws/                  # AWS services (Glue, Athena, etc.)
│   ├── azure/                # Azure services
│   ├── gcp/                  # GCP services
│   └── ...
├── examples/                 # Example usage
├── docs/                     # Additional documentation
├── tests/                    # Test suite (TODO)
└── pyproject.toml           # Package configuration
```

## Adding a New Integration

To add support for a new data source:

### 1. Create Integration Directory

```bash
mkdir -p dagster_insights/newsource
touch dagster_insights/newsource/__init__.py
touch dagster_insights/newsource/insights_newsource_resource.py
touch dagster_insights/newsource/newsource_utils.py
```

### 2. Implement Resource Wrapper

Follow the pattern from existing integrations:

```python
# insights_newsource_resource.py
from dagster import AssetObservation
from dagster_insights.insights_utils import get_current_context_and_asset_key

class InsightsNewSourceResource:
    """Resource wrapper that tracks costs for NewSource operations."""

    def __init__(self, connection_params):
        # Initialize connection
        pass

    @contextmanager
    def get_client(self):
        """Get wrapped client that tracks operations."""
        context, asset_key = get_current_context_and_asset_key()

        # Create client
        client = ...

        # Wrap to track operations
        wrapped_client = ...

        yield wrapped_client

        # After operations, emit cost observation
        context.log_event(
            AssetObservation(
                asset_key=asset_key,
                metadata={
                    "cost": ...,
                    # other metadata
                }
            )
        )
```

### 3. Add Utility Functions

```python
# newsource_utils.py
def meter_newsource_query(context, query, associated_asset_key=None):
    """Tag query with opaque ID for cost tracking."""
    opaque_id = context.get_opaque_id()
    # Add opaque ID to query as comment or tag
    return tagged_query
```

### 4. Export from Main Package

```python
# dagster_insights/__init__.py
try:
    from dagster_insights.newsource.insights_newsource_resource import (
        InsightsNewSourceResource as InsightsNewSourceResource,
    )
except ImportError:
    pass
```

### 5. Add Dependencies

```toml
# pyproject.toml
[project.optional-dependencies]
newsource = [
    "newsource-client>=1.0.0",
]
```

### 6. Create Example

```python
# examples/newsource_example.py
from dagster import asset, Definitions
from dagster_insights import InsightsNewSourceResource

@asset
def my_newsource_asset(newsource: InsightsNewSourceResource):
    with newsource.get_client() as client:
        client.execute("...")

defs = Definitions(
    assets=[my_newsource_asset],
    resources={
        "newsource": InsightsNewSourceResource(...),
    }
)
```

## Code Style

- Use **Black** for formatting: `black dagster_insights/`
- Use **Ruff** for linting: `ruff check dagster_insights/`
- Use **MyPy** for type checking: `mypy dagster_insights/`
- Follow existing patterns in the codebase

## Testing

```bash
# Run tests (once implemented)
pytest tests/

# Run with coverage
pytest --cov=dagster_insights tests/
```

## Documentation

- Update README.md if adding new integration
- Add docstrings to all public functions and classes
- Add examples to examples/ directory
- Update docs/ as needed

## Pull Request Process

1. **Fork** the repository
2. **Create a branch** for your feature: `git checkout -b feature/new-integration`
3. **Make your changes** following the patterns above
4. **Test your changes** (if tests exist)
5. **Commit** with clear message: `git commit -m "Add NewSource integration"`
6. **Push** to your fork: `git push origin feature/new-integration`
7. **Open a Pull Request** with description of changes

## Questions?

- Open an issue: https://github.com/eric-thomas-dagster/dagster_plus_cost_insights/issues
- Start a discussion: https://github.com/eric-thomas-dagster/dagster_plus_cost_insights/discussions

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
