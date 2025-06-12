### Scenario
`etl_job.py` processes ~5 GB CSV input and crashes with **OutOfMemoryError: Java heap space**.

### Objective
Refactor the job to avoid driver OOM **without changing cluster size**. Hint: remove `collect()` and keep transformations on the cluster.