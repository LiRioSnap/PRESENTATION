---
theme: "black"
transition: "slide"
customTheme: "presentation-theme"
transitionSpeed: "slow"
revealOptions:
    center: false
---

# Databricks associate engineer cert

**Troubleshooting, optimisation, and monitoring**

### Plan:
- A little bit on how things work {.fragment}
    - Driver vs Executor vs Partitions
- Basic optimisations {.fragment}
    - Liquid Clustering
    - AQE    
    - Broadcast join
- Common errors {.fragment}
    - Data skew
    - Shuffling
    - Disk Spilling
- Diagnosing failures {.fragment}
    - Startup
    - Library
    - OOM
- Working through an example {.fragment}
- Past paper questions {.fragment}


note: 
The aim will be to go through these sections alongside some questions in order to get an understanding of troubleshooting, optimisation, and monitoring. Unfortunately we do have to understand a little bit about some technical parts in order to get our heads round this. In particular, we will need to learn about drivers, executors, and partitions. 
---

## How things work

**Drivers, Executors, and Partitions**

> Drivers orchestrate tasks and collect all the results back together at the end {.fragment}

There is typically one driver node {.fragment}

> Executors actually carry out the tasks {.fragment}

There will usually be many executors and the driver node tells all of them what to do {.fragment}

> Partitions are the chunks of data that tasks get performed on {.fragment}

The data will already have been partitioned before our job {.fragment}

--

## Very high level analogy

I am a CEO of a big (but very old fashioned) company. I want to know how many sales we made this year and need to report this to my shareholders.  I keep my data in log books on shelves in my office. There is a log book per month.  I have 3 employees, Jake, Yusuf, and Natthaya

I tell Jake to add up the total for the first 4 months. I tell Yusuf to add up the total for months 5-8 and Natthaya to add up the total for months 9-12.

> I am the driver

> Jake, Yusuf, and Natthaya are my executors

> Each log book (for a given week) is a partition

Each of my executors reports their number back to me. I add them up and report that number to my stakeholders.

---

## A code based example

Suppose we have some big dataset of orders called FCT_ORDERS which looks like


| ORDER_ID | CUSTOMER_ID | PRODUCT_ID | DATE_KEY |
|----------|-------------|------------|----------|
| 1        |            7|           2|     20260721|
| 2        |            104| 206| 20260721|
| 3 | 7 | 1004 | 20260720|
| ... | ... | ... | ...|

and a products table DIM_PRODUCTS

| ID | COST | 
|------------|------|
| 1 | 1.70|
| 2 | 3.65|
|...|...|

--

## Problem 
Let's suppose we want a new table which shows how much money we have made off each product.

```python
from pyspark.sql import functions as F

orders = spark.table("fct_orders")
products = spark.table("dim_products")

result = (
    orders
    .join(products, orders.product_id == products.id, "inner")
    .groupBy("product_id")
    .agg(
        F.sum("COST").alias("total_amount")
    )
    .collect()
)
```

Let's consider how this actually gets executed.

--

The data in the orders table will most likely be partitioned by ORDER_ID. The data in the product table will most likely be partitioned by PRODUCT_ID.

This means an order for `PRODUCT_ID = 2` and the row describing `PRODUCT_ID = 2` in `DIM_PRODUCTS` could easily sit on two different executors — Spark has no reason to have kept them together.

--

## The join forces a shuffle

To join on `product_id`, every row from both tables needs to be compared against rows with the same key. Spark can't do that if matching rows are scattered across different executors.

So before the join can happen, Spark **shuffles** the data: it hashes `product_id` on both tables and redistributes rows across the cluster so that all rows sharing a `product_id` end up on the same executor.

This is a network operation — data physically moves machine to machine. It's usually the most expensive part of a query like this.

--

## The groupBy needs the same thing

`groupBy("product_id")` has the same requirement: to sum `COST` per product, every row for a given `product_id` has to be summed on the same executor.

If the shuffle from the join already grouped rows by `product_id`, Spark can often reuse that partitioning. If not, it triggers a second shuffle.

Either way — the driver isn't doing this work. It's the executors, exchanging data with each other, that carry out the shuffle and compute each partial sum.

--

## Who does what

- **Driver** — reads your Python code and builds the plan (this join, then this groupBy, then this agg). It doesn't touch a single row of data itself.
- **Executors** — hold the actual partitions of `fct_orders` and `dim_products`, perform the shuffle, and compute the per-product sums in parallel.
- **Partitions** — the chunks of data being moved and processed. A partition never splits across executors; it's reassigned as a whole when the shuffle happens.

--

## collect() brings it back to the driver

Once every executor has computed its slice of `total_amount` per `product_id`, `.collect()` tells them to send those results back to the driver, which assembles them into a single Python list.

This only works because the *result* — one row per product — is small. If we tried to `.collect()` the joined `orders` + `products` table before aggregating, we'd be asking the driver to hold every order row in its own memory, which is a common way to crash a Spark job.
---

## A query

```python
result = (
    df.groupBy("product_id")
        .count()
        .collect()
)
```

