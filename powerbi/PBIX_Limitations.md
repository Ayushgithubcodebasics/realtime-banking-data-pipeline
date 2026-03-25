# PBIX Limitation Note

This repo includes a full Power BI rebuild blueprint, DAX measures, theme JSON, and serving models.

A fully re-authored `.pbix` file is not bundled here because:

- PBIX internals are partly binary/proprietary
- safe validation requires Power BI Desktop
- this environment cannot open, refresh, save, and verify a modified PBIX end-to-end

Use the included serving models and rebuild the report in Power BI Desktop using:

- `powerbi/Measures_DAX.md`
- `powerbi/Report_Blueprint.md`
- `powerbi/Banking_Theme.json`
