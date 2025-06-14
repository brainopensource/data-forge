 R

Both Arrow and DuckDB support dplyr pipelines for people more comfortable with using dplyr for their data analysis. The Arrow package includes two helper functions that allow us to pass data back and forth between Arrow and DuckDB (to_duckdb() and to_arrow()). This is especially useful in cases where something is supported in one of Arrow or DuckDB but not the other. For example, if you find a complex dplyr pipeline where the SQL translation doesn't work with DuckDB, use to_arrow() before the pipeline to use the Arrow engine. Or, if you have a function (e.g., windowed aggregates) that aren't yet implemented in Arrow, use to_duckdb() to use the DuckDB engine. All while not paying any cost to (re)serialize the data when you pass it back and forth!

library(duckdb)
library(arrow)
library(dplyr)

# Open dataset using year, month folder partition
ds <- arrow::open_dataset("nyc-taxi", partitioning = c("year", "month"))

ds %>%
  # Look only at 2015 on, where the number of passenger is positive, the trip distance is
  # greater than a quarter mile, and where the fare amount is positive
  filter(year > 2014 & passenger_count > 0 & trip_distance > 0.25 & fare_amount > 0) %>%
  # Pass off to DuckDB
  to_duckdb() %>%
  group_by(passenger_count) %>%
  mutate(tip_pct = tip_amount / fare_amount) %>%
  summarise(
    fare_amount = mean(fare_amount, na.rm = TRUE),
    tip_amount = mean(tip_amount, na.rm = TRUE),
    tip_pct = mean(tip_pct, na.rm = TRUE)
  ) %>%
  arrange(passenger_count) %>%
  collect()

Python

The workflow in Python is as simple as it is in R. In this example we use DuckDB's Relational API.

import duckdb
import pyarrow as pa
import pyarrow.dataset as ds

# Open dataset using year, month folder partition
nyc = ds.dataset('nyc-taxi/', partitioning=["year", "month"])

# We transform the nyc dataset into a DuckDB relation
nyc = duckdb.arrow(nyc)

# Run same query again
nyc.filter("year > 2014 & passenger_count > 0 & trip_distance > 0.25 & fare_amount > 0")
    .aggregate("SELECT avg(fare_amount), avg(tip_amount), avg(tip_amount / fare_amount) AS tip_pct", "passenger_count").arrow()

DuckDB and Arrow: The Basics

In this section, we will look at some basic examples of the code needed to read and output Arrow tables in both Python and R.
Setup

First we need to install DuckDB and Arrow. The installation process for both libraries is shown below.

Python:

pip install duckdb
pip install pyarrow

R:

install.packages("duckdb")
install.packages("arrow")

To execute the sample examples in this section, we need to download the following custom Parquet files:

    integers.parquet
    lineitemsf1.snappy.parquet

Python

There are two ways in Python of querying data from Arrow.

    Through the Relational API:

     # Reads Parquet File to an Arrow Table
     arrow_table = pq.read_table('integers.parquet')

     # Transforms Arrow Table -> DuckDB Relation
     rel_from_arrow = duckdb.arrow(arrow_table)

     # we can run a SQL query on this and print the result
     print(rel_from_arrow.query('arrow_table', 'SELECT sum(data) FROM arrow_table WHERE data > 50').fetchone())

     # Transforms DuckDB Relation -> Arrow Table
     arrow_table_from_duckdb = rel_from_arrow.arrow()

By using replacement scans and querying the object directly with SQL:

 # Reads Parquet File to an Arrow Table
 arrow_table = pq.read_table('integers.parquet')

 # Gets Database Connection
 con = duckdb.connect()

 # we can run a SQL query on this and print the result
 print(con.execute('SELECT sum(data) FROM arrow_table WHERE data > 50').fetchone())

 # Transforms Query Result from DuckDB to Arrow Table
 # We can directly read the arrow object through DuckDB's replacement scans.
 con.execute("SELECT * FROM arrow_table").fetch_arrow_table()

It is possible to transform both DuckDB Relations and Query Results back to Arrow.
R

In R, you can interact with Arrow data in DuckDB by registering the table as a view (an alternative is to use dplyr as shown above).

library(duckdb)
library(arrow)
library(dplyr)

# Reads Parquet File to an Arrow Table
arrow_table <- arrow::read_parquet("integers.parquet", as_data_frame = FALSE)

# Gets Database Connection
con <- dbConnect(duckdb::duckdb())

# Registers arrow table as a DuckDB view
arrow::to_duckdb(arrow_table, table_name = "arrow_table", con = con)

# we can run a SQL query on this and print the result
print(dbGetQuery(con, "SELECT sum(data) FROM arrow_table WHERE data > 50"))

# Transforms Query Result from DuckDB to Arrow Table
result <- dbSendQuery(con, "SELECT * FROM arrow_table")

Streaming Data from/to Arrow

In the previous section, we depicted how to interact with Arrow tables. However, Arrow also allows users to interact with the data in a streaming fashion. Either consuming it (e.g., from an Arrow Dataset) or producing it (e.g., returning a RecordBatchReader). And of course, DuckDB is able to consume Datasets and produce RecordBatchReaders. This example uses the NYC Taxi Dataset, stored in Parquet files partitioned by year and month, which we can download through the Arrow R package:

arrow::copy_files("s3://ursa-labs-taxi-data", "nyc-taxi")

Python

# Reads dataset partitioning it in year/month folder
nyc_dataset = ds.dataset('nyc-taxi/', partitioning=["year", "month"])

# Gets Database Connection
con = duckdb.connect()

query = con.execute("SELECT * FROM nyc_dataset")
# DuckDB's queries can now produce a Record Batch Reader
record_batch_reader = query.fetch_record_batch()
# Which means we can stream the whole query per batch.
# This retrieves the first batch
chunk = record_batch_reader.read_next_batch()

R

# Reads dataset partitioning it in year/month folder
nyc_dataset = open_dataset("nyc-taxi/", partitioning = c("year", "month"))

# Gets Database Connection
con <- dbConnect(duckdb::duckdb())

# We can use the same function as before to register our arrow dataset
duckdb::duckdb_register_arrow(con, "nyc", nyc_dataset)

res <- dbSendQuery(con, "SELECT * FROM nyc", arrow = TRUE)
# DuckDB's queries can now produce a Record Batch Reader
record_batch_reader <- duckdb::duckdb_fetch_record_batch(res)

# Which means we can stream the whole query per batch.
# This retrieves the first batch
cur_batch <- record_batch_reader$read_next_batch()

The preceding R code shows in low-level detail how the data is streaming. We provide the helper to_arrow() in the Arrow package which is a wrapper around this that makes it easy to incorporate this streaming into a dplyr pipeline.

    In Arrow 6.0.0, to_arrow() currently returns the full table, but will allow full streaming in our upcoming 7.0.0 release.

Benchmark Comparison

Here we demonstrate in a simple benchmark the performance difference between querying Arrow datasets with DuckDB and querying Arrow datasets with Pandas. For both the Projection and Filter pushdown comparison, we will use Arrow tables. That is due to Pandas not being capable of consuming Arrow stream objects.

For the NYC Taxi benchmarks, we used a server in the SciLens cluster and for the TPC-H benchmarks, we used a MacBook Pro with an M1 CPU. In both cases, parallelism in DuckDB was used (which is now on by default).

For the comparison with Pandas, note that DuckDB runs in parallel, while pandas only support single-threaded execution. Besides that, one should note that we are comparing automatic optimizations. DuckDB's query optimizer can automatically push down filters and projections. This automatic optimization is not supported in pandas, but it is possible for users to manually perform some of these predicate and filter pushdowns by manually specifying them in the read_parquet() call.
Projection Pushdown

In this example we run a simple aggregation on two columns of our lineitem table.

# DuckDB
lineitem = pq.read_table('lineitemsf1.snappy.parquet')
con = duckdb.connect()

# Transforms Query Result from DuckDB to Arrow Table
con.execute("""SELECT sum(l_extendedprice * l_discount) AS revenue
                FROM
                lineitem;""").fetch_arrow_table()

# Pandas
arrow_table = pq.read_table('lineitemsf1.snappy.parquet')

# Converts an Arrow table to a Dataframe
df = arrow_table.to_pandas()

# Runs aggregation
res =  pd.DataFrame({'sum': [(df.l_extendedprice * df.l_discount).sum()]})

# Creates an Arrow Table from a Dataframe
new_table = pa.Table.from_pandas(res)

Name 	Time (s)
DuckDB 	0.19
Pandas 	2.13

The lineitem table is composed of 16 columns, however, to execute this query only two columns l_extendedprice and l_discount are necessary. Since DuckDB can push down the projection of these columns, it is capable of executing this query about one order of magnitude faster than Pandas.
Filter Pushdown

For our filter pushdown we repeat the same aggregation used in the previous section, but add filters on 4 more columns.

# DuckDB
lineitem = pq.read_table('lineitemsf1.snappy.parquet')

# Get database connection
con = duckdb.connect()

# Transforms Query Result from DuckDB to Arrow Table
con.execute("""SELECT sum(l_extendedprice * l_discount) AS revenue
        FROM
            lineitem
        WHERE
            l_shipdate >= CAST('1994-01-01' AS date)
            AND l_shipdate < CAST('1995-01-01' AS date)
            AND l_discount BETWEEN 0.05
            AND 0.07
            AND l_quantity < 24; """).fetch_arrow_table()

# Pandas
arrow_table = pq.read_table('lineitemsf1.snappy.parquet')

df = arrow_table.to_pandas()
filtered_df = lineitem[
        (lineitem.l_shipdate >= "1994-01-01") &
        (lineitem.l_shipdate < "1995-01-01") &
        (lineitem.l_discount >= 0.05) &
        (lineitem.l_discount <= 0.07) &
        (lineitem.l_quantity < 24)]

res =  pd.DataFrame({'sum': [(filtered_df.l_extendedprice * filtered_df.l_discount).sum()]})
new_table = pa.Table.from_pandas(res)

Name 	Time (s)
DuckDB 	0.04
Pandas 	2.29

The difference now between DuckDB and Pandas is more drastic, being two orders of magnitude faster than Pandas. Again, since both the filter and projection are pushed down to Arrow, DuckDB reads less data than Pandas, which can't automatically perform this optimization.
Streaming

As demonstrated before, DuckDB is capable of consuming and producing Arrow data in a streaming fashion. In this section we run a simple benchmark, to showcase the benefits in speed and memory usage when comparing it to full materialization and Pandas. This example uses the full NYC taxi dataset which you can download

# DuckDB
# Open dataset using year, month folder partition
nyc = ds.dataset('nyc-taxi/', partitioning=["year", "month"])

# Get database connection
con = duckdb.connect()

# Run query that selects part of the data
query = con.execute("SELECT total_amount, passenger_count, year FROM nyc where total_amount > 100 and year > 2014")

# Create Record Batch Reader from Query Result.
# "fetch_record_batch()" also accepts an extra parameter related to the desired produced chunk size.
record_batch_reader = query.fetch_record_batch()

# Retrieve all batch chunks
chunk = record_batch_reader.read_next_batch()
while len(chunk) > 0:
    chunk = record_batch_reader.read_next_batch()

# Pandas
# We must exclude one of the columns of the NYC dataset due to an unimplemented cast in Arrow.
working_columns = ["vendor_id","pickup_at","dropoff_at","passenger_count","trip_distance","pickup_longitude",
    "pickup_latitude","store_and_fwd_flag","dropoff_longitude","dropoff_latitude","payment_type",
    "fare_amount","extra","mta_tax","tip_amount","tolls_amount","total_amount","year", "month"]

# Open dataset using year, month folder partition
nyc_dataset = ds.dataset(dir, partitioning=["year", "month"])
# Generate a scanner to skip problematic column
dataset_scanner = nyc_dataset.scanner(columns=working_columns)

# Materialize dataset to an Arrow Table
nyc_table = dataset_scanner.to_table()

# Generate Dataframe from Arow Table
nyc_df = nyc_table.to_pandas()

# Apply Filter
filtered_df = nyc_df[
    (nyc_df.total_amount > 100) &
    (nyc_df.year >2014)]

# Apply Projection
res = filtered_df[["total_amount", "passenger_count","year"]]

# Transform Result back to an Arrow Table
new_table = pa.Table.from_pandas(res)

Name 	Time (s) 	Peak memory usage (GBs)
DuckDB 	0.05 	0.3
Pandas 	146.91 	248

The difference in times between DuckDB and Pandas is a combination of all the integration benefits we explored in this article. In DuckDB the filter pushdown is applied to perform partition elimination (i.e., we skip reading the Parquet files where the year is <= 2014). The filter pushdown is also used to eliminate unrelated row_groups (i.e., row groups where the total amount is always <= 100). Due to our projection pushdown, Arrow only has to read the columns of interest from the Parquet files, which allows it to read only 4 out of 20 columns. On the other hand, Pandas is not capable of automatically pushing down any of these optimizations, which means that the full dataset must be read. This results in the 4 orders of magnitude difference in query execution time.

In the table above, we also depict the comparison of peak memory usage between DuckDB (Streaming) and Pandas (Fully-Materializing). In DuckDB, we only need to load the row group of interest into memory. Hence our memory usage is low. We also have constant memory usage since we only have to keep one of these row groups in-memory at a time. Pandas, on the other hand, has to fully materialize all Parquet files when executing the query. Because of this, we see a constant steep increase in its memory consumption. The total difference in memory consumption of the two solutions is around 3 orders of magnitude.