For this assignment, I focused on creating infrastructure that is scalable and robust enough to support the envisioned pipeline.

Database backend: While a file-based system based on in-memory processing could work for the present task, this pipeline should be implement with a proper database for scalability. I used DuckDB here for portability and simplicity, but I would prefer PostGreSQL with PostGIS for the final implementation. DuckDB's spatial extension is powerful enough for the spatial processing aspects of this task and supports (somewhat limited) spatial indices.

building_damage module: In this module, I

