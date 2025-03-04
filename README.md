# ragster

I wanted to try building a very basic RAG pipeline with Dagster Components. I think that Components could be a very compelling tool for AI engineers, who may not want to learn a lot about Dagster. In fact, it could be the easiest way to prototype a new AI product and ship it to production.

In order to do this I had to do a few things:

1. Ingest data from somewhere. I decided to pull in GitHub issues. PyAirbyte seems like a decent tool for this and is currently targeting AI use cases. We might want to consider shipping a PyAirbyte component as part of Embedded ELT since it was super easy to get running.

2. Store the data in a vector store. I chose ChromaDB since it was easy to get running, it includes a simple embedding model, and I know the founders.

3. Pass data between these two components. This was the big challenge. Right now we don't have any standard way to compose two Components together. IOManagers were the previous Dagster attempt at this, but they're really only appropriate for data that fits in memory and will be used exclusively in Python.

## Passing data between components

I built a little thing called `daglake`. It lets Components pass data between each other. The main idea is that all components agree to share data via an open datalake using the same protocols and formats. In this case, it's Parquet files on S3-compatible storage or local disk.

You plug in a `daglake` resource which contains the credentials for the datalake and can generate paths for each asset.
