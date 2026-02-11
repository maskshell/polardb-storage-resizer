# Tag Filters Configuration Tests - RED Phase

## Overview
This document summarizes the RED phase test implementation for `cluster_tag_filters` configuration.

## Test File
- **Location**: `/Users/solosus/dev/ws-python/polardb-storage-resizer/tests/test_config.py`
- **Test Class**: `TestClusterTagFilters`

## Test Results

### Summary
- **Total Tests**: 7
- **Passed**: 4 (tests that don't require env var parsing)
- **Failed**: 3 (tests that require env var parsing - expected in RED phase)

### Test Cases

#### ✅ Passing Tests (4)
These tests pass because the functionality already exists or doesn't depend on env var parsing:

1. `test_from_env_with_empty_tag_filters`
   - Verifies empty dict when env var not set
   - Works because cluster_tag_filters defaults to `{}`

2. `test_from_yaml_with_tag_filters`
   - Verifies YAML loading of cluster_tag_filters
   - Works because `from_yaml()` already supports this field

3. `test_tag_filters_default_empty`
   - Verifies default value is empty dict
   - Works because field has `default_factory=dict`

4. `test_tag_filters_can_be_set_directly`
   - Verifies cluster_tag_filters can be set via constructor
   - Works because it's a standard dataclass field

#### ❌ Failing Tests (3) - Expected in RED Phase
These tests fail because `from_env()` doesn't support `CLUSTER_TAG_FILTERS`:

1. `test_from_env_with_tag_filters`
   - **Expected**: Parse `CLUSTER_TAG_FILTERS=Environment:production,Team:backend`
   - **Actual**: Returns empty dict `{}`
   - **Reason**: `from_env()` doesn't parse `CLUSTER_TAG_FILTERS` env var

2. `test_from_env_with_invalid_tag_format`
   - **Expected**: Skip invalid entries (without colon), keep valid ones
   - **Actual**: Returns empty dict `{}`
   - **Reason**: `from_env()` doesn't parse `CLUSTER_TAG_FILTERS` env var

3. `test_from_env_with_colon_in_value`
   - **Expected**: Handle values with colons (e.g., `url:https://example.com`)
   - **Actual**: Returns empty dict `{}`
   - **Reason**: `from_env()` doesn't parse `CLUSTER_TAG_FILTERS` env var

## Required Implementation (GREEN Phase)

To make these tests pass, update `config.py` `from_env()` method to:

1. Add helper function to parse `dict[str, str]` from env var:
   ```python
   def get_env_dict(key: str) -> dict[str, str]:
       """Parse key:value pairs from environment variable."""
       value = os.environ.get(key, "")
       if not value:
           return {}

       result = {}
       for item in value.split(","):
           item = item.strip()
           if ":" in item:
               # Split on first colon only (value can contain colons)
               k, v = item.split(":", 1)
               result[k.strip()] = v.strip()
       return result
   ```

2. Add `cluster_tag_filters` to the return statement:
   ```python
   return cls(
       ...
       cluster_whitelist=get_env_list("CLUSTER_WHITELIST"),
       cluster_blacklist=get_env_list("CLUSTER_BLACKLIST"),
       cluster_tag_filters=get_env_dict("CLUSTER_TAG_FILTERS"),  # ADD THIS
   )
   ```

## Format Specification

### Environment Variable Format
```
CLUSTER_TAG_FILTERS=key1:value1,key2:value2,key3:value3
```

**Rules**:
- Entries are comma-separated
- Each entry is `key:value` format
- Split on first colon only (values can contain colons)
- Invalid entries (no colon) are silently skipped
- Empty env var returns empty dict

### Examples

#### Basic Usage
```bash
export CLUSTER_TAG_FILTERS="Environment:production,Team:backend"
# Result: {"Environment": "production", "Team": "backend"}
```

#### Values with Colons
```bash
export CLUSTER_TAG_FILTERS="url:https://example.com,path:/api/v1"
# Result: {"url": "https://example.com", "path": "/api/v1"}
```

#### Invalid Entries Skipped
```bash
export CLUSTER_TAG_FILTERS="valid:value,invalid_entry,another:valid"
# Result: {"valid": "value", "another": "valid"}
# "invalid_entry" is skipped (no colon)
```

#### Empty Value
```bash
export CLUSTER_TAG_FILTERS=""
# Result: {}
```

## Next Steps

1. **GREEN Phase**: Implement env var parsing in `config.py`
2. **REFACTOR Phase**: Review implementation for edge cases and optimization
3. **Documentation**: Update docstrings and README with new env var

## Verification Commands

```bash
# Run only tag filter tests
uv run pytest tests/test_config.py::TestClusterTagFilters -v

# Run all config tests
uv run pytest tests/test_config.py -v

# Run with coverage
uv run pytest tests/test_config.py --cov=polardb_storage_resizer.config --cov-report=term-missing
```
