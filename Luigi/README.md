# Luigi Pipeline Orchestration

## Overview

This directory contains the orchestration logic for the NYC Taxi data pipeline using **Luigi**.

Luigi is an open-source Python workflow manager that allows developers to define pipelines as a series of dependent tasks. It automatically handles task execution order, dependency resolution, and pipeline scheduling.

In this project, Luigi is used to coordinate the execution of the data pipeline components, ensuring that each stage of the workflow runs in the correct order.

The Luigi pipeline orchestrates:

```
Spark ETL → Data written to GCS → Snowflake Data Warehouse → Tableau Analytics
```

---

# Pipeline Architecture

The Luigi workflow coordinates the execution of the data pipeline components.

```
Raw Data (GCS)
      │
      ▼
Spark Data Processing
      │
      ▼
Processed Tables in GCS
      │
      ▼
Snowflake Data Warehouse Loading
      │
      ▼
Analytics / Tableau Dashboards
```

Each step of the workflow is implemented as a Luigi **Task**.

Luigi tasks define:

- required upstream tasks
- the computation to run
- the output produced

Luigi ensures tasks run only after their dependencies complete successfully.

---

# Luigi Concepts Used

Luigi pipelines are built using three core concepts.

### Task

A `Task` represents a unit of work in the pipeline.

Tasks in this project include:

- submitting the Spark ETL pipeline to Dataproc
- loading processed outputs into Snowflake
- marking the full pipeline as complete

Each task defines:

```
requires()
run()
output()
```

These methods allow Luigi to determine dependencies and manage execution order.

---

### Target

A `Target` represents the output of a task.

Typical targets include:

```
files in GCS
database tables
local files
```

Luigi checks if a target already exists before executing a task.  
If the output exists, the task is skipped.

---

### Dependency Resolution

Tasks declare dependencies using:

```
requires()
```

Luigi automatically constructs the pipeline execution graph and ensures tasks run in the correct order.

---

# Pipeline Implementation

The Luigi pipeline script defines tasks responsible for orchestrating the workflow.

The current Luigi implementation defines:

```
RunSparkJob
LoadToSnowflake
FullPipeline
```

Each task executes the required step in the data pipeline and produces a target output.

The final task ensures the entire pipeline is completed successfully.

---

# Running the Pipeline

The repository is intended to be run from the root entrypoint:

```
./pipeline.py
```

This root script calls the Luigi tasks internally and automatically executes all required upstream tasks.

---

# Generated Files

The folder may contain compiled Python files generated automatically by the Python interpreter.

Example:

```
__pycache__/
pipeline.cpython-313.pyc
```

These files store compiled bytecode to speed up execution and are not part of the pipeline logic.

---

# Role in the Project

Luigi acts as the **pipeline orchestration layer**.

Responsibilities include:

- coordinating Spark processing
- managing execution order of pipeline steps
- handling pipeline dependencies
- enabling reproducible pipeline runs

This design separates the pipeline into three layers:

```
Spark        → Data Processing
Luigi        → Pipeline Orchestration
Snowflake    → Data Warehouse
```

This modular architecture improves maintainability and allows each component to evolve independently.
