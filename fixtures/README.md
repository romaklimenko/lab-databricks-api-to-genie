# Weather fixture

`weather.json` contains three public Open-Meteo API responses, fetched on October 9, 2026.
Request parameters and retrieval timestamp are stored in the file.

- Source: [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api).
- Provider: Open-Meteo, using ERA5 reanalysis from Copernicus/ECMWF.
- Licence: [CC BY 4.0](https://open-meteo.com/en/licence).
- Changes: the responses are wrapped with source metadata and city identifiers. Weather values are unchanged.
- Coverage: Copenhagen, Aarhus, and Odense, September 21-27, 2026.
- Daily boundaries: Europe/Copenhagen.
- Units: degrees Celsius, millimeters, and meters per second.
- SHA-256 of canonical snapshot JSON: `f026297918a402c4d0983db66a61634072e799efdd6b1b68460276bf7b6e7590`.

These values are reanalysis estimates. They are not direct measurements at each city center.
The committed snapshot supports repeatable tests despite possible upstream revisions.

Refresh intentionally with `uv run weather-lab fetch`. Review the fixture diff and update verification results before committing.
