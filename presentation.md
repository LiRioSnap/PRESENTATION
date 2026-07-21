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





---

## A query

```python
result = (
    df.groupBy("product_id")
        .count()
        .collect()
)
```

