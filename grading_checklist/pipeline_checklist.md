# Grading - Pipeline Design

https://datascience.stackexchange.com/questions/118627/when-is-the-right-moment-to-split-the-dataset#:~:text=It%27s%20better%20to%20split%20the,applied%20to%20the%20testing%20set.
https://www.reddit.com/r/dataengineering/comments/r0w9bg/dealing_with_schema_changes_with_an_existing/
https://www.montecarlodata.com/blog-pyspark-data-quality-checks


- [ ] How reusable is your data pipeline?
- [ ] A well-written ML pipeline should implement a sequence of data processing
operations to consume the input data, train the model
and output predictions for the validation and test data
- [ ] Visualise your pipeline and its operations on the data with a diagram
- [ ] How much (manual) effort would it be to update your pipeline
if the input data (or even its schema) changed?
- [ ] How did you decide which parts of the pipeline to run in DuckDB / PySpark?
- [ ] Your pipeline should use DuckDB and/or (Py)Spark in appropriate parts
- [ ] You should be able to explain why (or why not) it makes sense to use DuckDB
and/or (Py)Spark in a given part (e.g., based on learnings from the course)
