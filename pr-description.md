# Pull Request Description

## 📋 Summary

This PR introduces a comprehensive **access layer** that automatically converts API responses to pandas DataFrames, significantly improving the developer experience for data analysis workflows. The access layer sits on top of the existing API clients and provides automatic data normalization, column renaming, and type inference, making BDL data immediately ready for analysis. Additionally, this PR enhances API clients with improved parameter handling, pagination controls, and configuration flexibility.

## 🎯 Purpose & Context

The Local Data Bank (BDL) API returns data in JSON format with camelCase field names and nested structures, which requires manual conversion and normalization before analysis. This PR addresses this by introducing a dedicated access layer that:

- Automatically converts API responses to pandas DataFrames
- Normalizes column names from camelCase to snake_case
- Infers proper data types (integers, floats, booleans)
- Flattens nested data structures into tabular format

This change enables users to work with BDL data more efficiently, reducing boilerplate code and making the library more accessible to data analysts and scientists.

## 🔧 Changes Made

### Access Layer Implementation
- **New `pybdl.access` module**: Introduced a complete access layer with classes for all API endpoints:
  - `AggregatesAccess`, `AttributesAccess`, `DataAccess`, `LevelsAccess`, `MeasuresAccess`, `SubjectsAccess`, `UnitsAccess`, `VariablesAccess`, `YearsAccess`
- **BaseAccess class**: Provides common functionality for DataFrame conversion, column renaming, and data normalization
- **Dual interface design**: Main `BDL` client now exposes both:
  - **Access layer** (default): `bdl.levels`, `bdl.data`, etc. → Returns DataFrames
  - **API layer**: `bdl.api.levels`, `bdl.api.data`, etc. → Returns raw dictionaries

### API Client Enhancements
- **Parameter improvements**: 
  - Renamed `year` → `years` across methods for consistency (supports multiple years)
  - Renamed `variable_id` → `variable_ids` in data retrieval methods (supports lists)
  - Removed `all_pages` parameter in favor of `max_pages` for clearer pagination control
- **Format enum**: Introduced `Format` enum (JSON, JSONAPI, XML) for response format handling
- **Default page size**: Added configurable `page_size` parameter (default: 100) for paginated requests
- **Enhanced request handling**: Improved parameter and header management across all API clients

### Configuration & Client Updates
- **Flexible initialization**: `BDL` client now accepts `None`, `dict`, or `BDLConfig` instances
- **Configuration enhancements**: Added `page_size` and default `format` to `BDLConfig`
- **Environment variable support**: Enhanced handling for configuration overrides

### Testing Infrastructure
- **Test organization**: Reorganized tests into `unit/`, `integration/`, and `e2e/` directories
- **Test markers**: Added custom pytest markers (`@pytest.mark.unit`, `@pytest.mark.integration`, `@pytest.mark.e2e`)
- **Comprehensive coverage**:
  - Unit tests for all access layer classes
  - Integration tests with sample data for all endpoints
  - End-to-end workflow tests
- **Sample data**: Added sample JSON responses for integration testing

### Documentation
- **New documentation**: Added comprehensive `access_layer.rst` documentation
- **Updated guides**: Enhanced `main_client.rst`, `api_clients.rst`, and `config.rst`
- **Examples notebook**: Added `examples.ipynb` with practical usage examples
- **Appendix**: Added technical implementation details for developers

### Dependencies & Infrastructure
- **MyST Notebook**: Added for documentation support
- **CI updates**: Upgraded python-semantic-release action (v9.15.0 → v10.5.2)
- **Gitignore**: Updated to exclude IDE and environment files

## ✅ Testing

### Test Coverage
- **Unit tests**: 9 new test files covering all access layer classes (aggregates, attributes, data, levels, measures, subjects, units, variables, years)
- **Integration tests**: 9 integration test files with realistic sample data
- **End-to-end tests**: 2 workflow tests covering complete user scenarios
- **API client tests**: Updated existing tests to reflect parameter changes

### Test Execution
```bash
# Run all tests
pytest

# Run by category
pytest -m unit
pytest -m integration
pytest -m e2e
```

### Manual Testing
1. **Access layer DataFrame conversion**:
   ```python
   from pybdl import BDL, BDLConfig
   bdl = BDL(BDLConfig(api_key="your-key"))
   df = bdl.levels.list_levels()
   assert isinstance(df, pd.DataFrame)
   assert 'level_id' in df.columns  # camelCase → snake_case
   ```

2. **API layer still returns raw dicts**:
   ```python
   raw = bdl.api.levels.list_levels()
   assert isinstance(raw, dict)
   ```

3. **Parameter changes**:
   - Verify `years` parameter accepts lists
   - Verify `variable_ids` parameter accepts lists
   - Verify `max_pages` controls pagination correctly

## 🚨 Breaking Changes & Migration Notes

### Parameter Renames
- **`year` → `years`**: Update calls to `get_data_by_variable()`, `get_data_by_unit()`, and related methods
  ```python
  # Old
  bdl.api.data.get_data_by_variable(variable_id="3643", year=2021)
  
  # New
  bdl.api.data.get_data_by_variable(variable_id="3643", years=[2021])
  ```

- **`variable_id` → `variable_ids`**: Update calls to `get_data_by_unit()` and `aget_data_by_unit()`
  ```python
  # Old
  bdl.api.data.get_data_by_unit(unit_id="123", variable_id="3643")
  
  # New
  bdl.api.data.get_data_by_unit(unit_id="123", variable_ids=["3643"])
  ```

### Removed Parameters
- **`all_pages` parameter**: Removed from `DataAPI`, `SubjectsAPI`, `UnitsAPI`, and `VariablesAPI`
  ```python
  # Old
  bdl.api.data.get_data_by_variable(variable_id="3643", all_pages=True)
  
  # New
  bdl.api.data.get_data_by_variable(variable_id="3643", max_pages=None)  # None = all pages
  ```

### Migration Path
1. **For existing code using API layer**: Update parameter names as shown above
2. **For new code**: Consider using the access layer (default interface) for DataFrame-based workflows
3. **For advanced use cases**: Continue using `bdl.api.*` for raw dictionary access

## 🔍 Review Focus Areas

### Critical Review Points
1. **DataFrame conversion logic**: Verify correctness of nested data flattening in `BaseAccess._to_dataframe()`
2. **Column renaming**: Check that `_column_renames` mappings are correctly applied across all access classes
3. **Pagination handling**: Ensure `max_pages` logic correctly handles edge cases (None, 0, negative values)
4. **Type inference**: Validate that data types are correctly inferred from API responses
5. **Backward compatibility**: Confirm that API layer changes don't break existing integrations

### Performance Considerations
- DataFrame conversion overhead for large responses
- Memory usage with nested data flattening
- Pagination efficiency with `max_pages` parameter

### Security & Configuration
- Verify API key handling remains secure
- Check that environment variable overrides work correctly
- Validate rate limiting still functions properly

## 📦 Dependencies & Side Effects

### New Dependencies
- **myst-nb**: Added for MyST Notebook support in documentation

### Updated Dependencies
- **python-semantic-release**: Upgraded in CI workflow (v9.15.0 → v10.5.2)

### Side Effects
- **Import paths**: No breaking changes to public API imports
- **Configuration**: New optional `page_size` and `format` config parameters (backward compatible)
- **Test organization**: Tests moved to `tests/unit/` directory (does not affect runtime)

## 🚀 Deployment Notes

### Pre-Deployment Checklist
- [ ] Verify all tests pass in CI
- [ ] Update CHANGELOG.md with breaking changes
- [ ] Update version number (semantic versioning)
- [ ] Review documentation for accuracy

### Post-Deployment
- **Documentation**: New documentation will be available at `/docs/access_layer.html`
- **Examples**: Jupyter notebook examples available in `docs/examples.ipynb`
- **User communication**: Consider announcing the new access layer in release notes

### Environment Considerations
- No database migrations required
- No infrastructure changes needed
- Backward compatible with existing API clients (with parameter updates)

## 📊 Statistics

- **Files changed**: 111 files
- **Lines added**: ~9,623 insertions
- **Lines removed**: ~1,311 deletions
- **Net change**: +8,312 lines
- **New test files**: 20+ test files
- **New access classes**: 9 classes
- **Documentation pages**: 4 new/updated pages
