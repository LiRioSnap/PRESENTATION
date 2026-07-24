---
theme: "black"
transition: "slide"
customTheme: "presentation-theme"
transitionSpeed: "slow"
revealOptions:
    width: 1600
    height: 900
    margin: 0.04
    center: false
---

# Databricks associate engineer cert

**Troubleshooting, optimisation, and monitoring**

### Plan:
- Drivers, Executors, Partitions {.fragment}
    - What they are
    - High level analogy
    - Code based example
- Storage vs Memory {.fragment}
    - What they are
    - High level analogy (continued)
    - Technical details
- Basic optimisations {.fragment}
    - Liquid Clustering
    - AQE    
    - Broadcast join
- Past paper questions {.fragment}

---

## How things work

**Drivers, Executors, and Partitions**

> Drivers orchestrate tasks and collect all the results back together at the end {.fragment}

There is typically one driver node {.fragment}

> Executors actually carry out the tasks {.fragment}

There will usually be many executors and the driver node tells all of them what to do {.fragment}

> Partitions are the chunks of data that tasks get performed on {.fragment}

The data will already have been partitioned before our job {.fragment}

---

## Very high level analogy

I am a CEO of a big (but very old fashioned) company. I want to know how many sales we made this year and need to report this to my shareholders.  I keep my data in log books on shelves in my office. There is a log book per product.  I have 3 employees: Jake, Yusuf, and Natthaya.

Imagine we have 90 products. It is too big a task for me to add all of them up myself. So I tell Jake to add up the total for the first 30 products. I tell Yusuf to add up the total for products 31-60 and Natthaya to add up the total for products 61-90.

> I am the driver. I don't actually do any of the tasks.

> Jake, Yusuf, and Natthaya are my executors

> Each log book (for a given product) is a partition

Partitions won't get split between different executors. I.e. I will never get Jake to do the total for some of the sales of product 1 and Natthaya to total the rest of product 1. 

--

## Executor OOM

Imagine one of my products sells a lot better than the others. Let's say it is product 37. We will have a lot more data for that product than any of the others. 

> This is data skew.

Yusuf has this partition as one of his partitions to do. Maybe he comes back to me and says this is simply too much stuff for him to do. 

> This is an executor OOM.

Notice that adding more executors wouldn't help. Instead we would need to break the bigger partition down into smaller partitions

> This is repartitioning

--

## Driver OOM

Suppose I move my headquarters to Bermuda for "tax reasons". Rather than getting them to total things up I get Jake, Yusuf, and Natthaya to convert each sale for its value in Bermudian Dollars. 

As before they will each take partitions of products. At the end I get them to send all this converted data back to me. 

 This is a problem. Each partition is small enough for the executors to handle but at the end when I recombine all of the partitions it is too much data for **me** to handle. 

 > This is a Driver OOM

 The solution is to get a better, smarter more advanced CEO who can handle this data

 > In compute terms this means increasing the Driver memory.





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

<br>
</br>

**Out of laziness we will assume that any order is an order of one item**

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

> The data in the orders table will most likely be partitioned by ORDER_ID. 

<br>

> The data in the product table will most likely be partitioned by PRODUCT_ID.

<br>

This means an order for PRODUCT_ID = 2 and the row describing PRODUCT_ID = 2 in DIM_PRODUCTS could easily sit on two different executors. Spark has no reason to have kept them together.

--

## The join forces a shuffle

To join on `product_id`, every row from both tables needs to be compared against rows with the same key. Spark can't do that if matching rows are scattered across different executors.

So before the join can happen, Spark **shuffles** the data: it redistributes rows across the cluster so that all rows sharing a `product_id` end up on the same executor.

--

## The groupBy needs the same thing

`groupBy("product_id")` has the same requirement: to sum `COST` per product, every row for a given `product_id` has to be summed on the same executor.

If the shuffle from the join already grouped rows by `product_id`, Spark can often reuse that partitioning. If not, it triggers a second shuffle.

Either way the driver isn't doing this work. It's the executors, exchanging data with each other, that carry out the shuffle and compute each partial sum.

--

## Who does what

- **Driver** reads your Python code and builds the plan (this join, then this groupBy, then this agg). It doesn't touch a single row of data itself.
- **Executors** hold the actual partitions of `fct_orders` and `dim_products`, perform the shuffle, and compute the per product sums in parallel.
- **Partitions** the chunks of data being moved and processed. A partition never splits across executors; it's reassigned as a whole when the shuffle happens.

--

## collect() brings it back to the driver

Once every executor has computed its slice of `total_amount` per `product_id`, `.collect()` tells them to send those results back to the driver, which assembles them into a single Python list.

**Useful for the exam:**

> .collect() and .toPandas() both send the data back to the driver node {.fragment}

> If the exam talks about a memory issue and also either .collect() or .toPandas() then think **Driver memory issue** {.fragment}

---

## Storage vs Memory

There two (terms and conditions apply) places that computers store data.

> Storage is bigger and is used for long term storage of information but is slower to read and right from.

<br>

> Memory is smaller and is used for data currently being used and is faster to read and right from.

--

## High level analogy continued

My company has continued to grow and I now have entire rooms filled with log books. I buy a warehouse (the physical kind) and put all of these log books on shelves in this warehouse. 

Let's imagine my offices are upstairs from this warehouse. I still have Jake, Yusuf, and Natthaya working on all of my tasks for me. In order to help them out I buy a small set of shelves which live upstairs in the office where they work. This means they can store small amounts of log books that they are currently working on in the office. This saves them the time of having to walk downstairs and look for this information and keep bringing it back up and down from the warehouse.

> The warehouse downstairs is storage (harddrive/disk)

<br>

> The small set of shelves in the office is memory (RAM)


--

## The technical details:

Each executor has its own memory and storage. If an executor is working on a task involving too much data for its allocated memory it has two options.

> If it is not too severe (in terms of size) then it can **spill** some of this data back into storage.

In this case the task still completes but is just slower because it is reading and writing to storage (disk).

> If it is a severe case (massively over the size of data in memory) this is when we see an executor OOM.

In this case the task fails and does not complete. We will then see an error message saying "Executor Lost".

Since they are caused by the same problem both of these have the same fix:

- Break partitions into smaller pieces
- Increase executor memory


---

## Optimisations

**Liquid clustering:** 

A predictive optimisation which updates clustering keys based on query patterns.

**AQE (Adaptive Query Execution):**

Databricks can change the way your queries execute when it thinks it could help. This includes switching to broadcast joins, repartitioning, and coalescing partitions. 

**Broadcast joins:**

Suppose we have a join which needs to happen between a big table and a very small table. Rather than having to shuffle around a lot of data instead we could create copies of the small table and send a copy to each executor (we **broadcast** the small table to the executors).

**Optimize:**

A command which compacts small files into larger ones.

**Vacuum:**

Clears out old logs of tables.

--

## Configuring broadcast joins:

A question they like asking is about 

> autoBroadcastJoinThreshold

Any tables <= this size will automatically be broadcast during joins. The default is 10MB (10485760 bytes)

This is just one of those things you have to know. (You don't need to know the default size, only that you manage it via autoBroadcastJoinThreshhold).

---

## Practice questions:

[Databricks practice exam](https://snapanalytics.udemy.com/course/practice-exams-databricks-certified-data-engineer-associate/learn/quiz/5731990?kw=databricks&src=sac#overview)


**Btw: don't forget to do timesheets today as per mark's email**



