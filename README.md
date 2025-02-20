# Pandas vs PySpark: A Comparison

This repository highlights the differences and similarities between **Pandas** (Python's data manipulation library) and **PySpark** (Apache Spark's Python API) for common data processing tasks. The examples are based on the `BigMart Sales` dataset.

---

## Key Comparisons

### 1. **Reading Data**
- **Pandas**: Reads CSV files into a DataFrame using `pd.read_csv()`.
- **PySpark**: Reads CSV files into a Spark DataFrame using `spark.read.csv()`.

### 2. **Schema Inspection**
- **Pandas**: Inspect column names and data types using `df.info()`.
- **PySpark**: Inspect schema using `df.printSchema()`.

### 3. **Column Selection**
- **Pandas**: Select columns using `df[['col1', 'col2']]`.
- **PySpark**: Select columns using `df.select(col('col1'), col('col2'))`.

### 4. **Filtering Data**
- **Pandas**: Filter rows using `df[df['col'] == value]`.
- **PySpark**: Filter rows using `df.filter(col('col') == value)`.

### 5. **Value Counts**
- **Pandas**: Count unique values using `df['col'].value_counts()`.
- **PySpark**: Count unique values using `df.groupBy('col').count()`.

### 6. **Handling Nulls**
- **Pandas**: Filter nulls using `df[df['col'].isnull()]`.
- **PySpark**: Filter nulls using `df.filter(col('col').isNull())`.

### 7. **Renaming Columns**
- **Pandas**: Rename columns using `df.rename(columns={'old': 'new'})`.
- **PySpark**: Rename columns using `df.withColumnRenamed('old', 'new')`.

### 8. **Adding Columns**
- **Pandas**: Add columns using `df['new_col'] = value`.
- **PySpark**: Add columns using `df.withColumn('new_col', lit(value))`.

### 9. **Data Type Conversion**
- **Pandas**: Convert data types using `df['col'].astype(new_type)`.
- **PySpark**: Convert data types using `df.withColumn('col', col('col').cast(new_type))`.

### 10. **Sorting**
- **Pandas**: Sort data using `df.sort_values(by='col', ascending=False)`.
- **PySpark**: Sort data using `df.sort(col('col').desc())`.

### 11. **Dropping Columns**
- **Pandas**: Drop columns using `df.drop(columns='col')`.
- **PySpark**: Drop columns using `df.drop('col')`.

---

## Key Differences
1. **Execution Model**:
   - Pandas operates in-memory and is eager (executes immediately).
   - PySpark is distributed and lazy (executes only when an action is called).

2. **Null Handling**:
   - PySpark includes `null` values in aggregations by default, while Pandas excludes them.

3. **Immutable DataFrames**:
   - PySpark operations return new DataFrames, whereas Pandas can modify DataFrames in-place.

4. **Syntax**:
   - PySpark uses method chaining (e.g., `groupBy().count().orderBy()`), while Pandas uses a mix of methods and attributes.

---

## Troubleshooting
1. **Java Errors**: Ensure `JAVA_HOME` is set correctly.
2. **Port Conflicts**: Configure Spark ports explicitly if needed.
3. **Data Type Mismatches**: Use `.cast()` in PySpark to enforce data types.

---

Use `spark.stop()` to gracefully shut down the Spark session after processing.