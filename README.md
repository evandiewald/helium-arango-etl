# helium-arango-etl
![build image](https://github.com/evandiewald/helium-arango-etl/actions/workflows/docker-image.yml/badge.svg)
![publish image](https://github.com/evandiewald/helium-arango-etl/actions/workflows/docker-publish.yml/badge.svg)

ETL service that converts relational blockchain data into a native graph format for storage in [ArangoDB](https://www.arangodb.com/).

## About
Helium's Blockchain API is an effective way to view historical data stored on-chain, but the ledger-based format is less useful for feeding directly into network models. In this project, we propose to build a framework for a graph-based representation of blockchain activity, including Proof of Coverage and Token Flow. By capturing the natural adjacency between hotspots and accounts, we will be able to build machine learning models to, for instance, identify likely "gaming" behavior and predict coverage maps based on hotspot placement. 

[More details in the full project proposal](https://github.com/dewi-alliance/grants/issues/23).

## Dependencies
To run helium-arango-etl, you will need:
- Read/write access to a running [ArangoDB instance](https://www.arangodb.com/download-major/docker/).
  - e.g. `docker run -d --name arango -p 8529:8529 -e ARANGO_ROOT_PASSWORD=openSesame arangodb/arangodb:3.8.2`
  - If running locally, you can view the Arango WebUI at [`http://localhost:8529/`](http://localhost:8529/)
- Read access to a PostgreSQL database populated by a [blockchain-etl](https://github.com/helium/blockchain-etl) node.

## Quick setup
1. Make a copy of `.env.template` called `.env` and include the URL's and credentials to access both databases.
2. Build the docker image with:

   `docker build -t helium-arango-etl:latest .`
3. Run the container with:

    `docker run -d --name etl helium-arango-etl`
4. To view logs:

    `docker exec etl tail -f logs/etl.log`

## Related Works

- [`Exploring the Helium Network with Graph Theory`](https://towardsdatascience.com/exploring-the-helium-network-with-graph-theory-66cbb8bffff9): Blog post inspiring much of this work.
- [`evandiewald/helium-arango-http`](https://github.com/evandiewald/helium-arango-http): an HTTP API to run queries on the data stored in the ArangoDB database populated by this ETL.
- [`evandiewald/helium-arango-analysis`](https://github.com/evandiewald/helium-arango-analysis): (coming soon) methods and models for running Graph Theory- and Graph Neural Network-based analyses of the Arango graphs using Python-friendly formats, such as networkx and torch-geometric.

## Contributing
Pull requests are welcome, especially when it comes to adding additional interesting queries. The focus of this project is to leverage the native graph format of ArangoDB to run analyses that are *not already covered by the [Blockchain API](https://docs.helium.com/api)*, such as token flow and coverage mapping. If you are not familiar with ArangoDB, the [AQL query language](https://www.arangodb.com/docs/stable/aql/) allows for powerful extraction of *adjacencies* in the dataset.

## Acknowledgements
This project is supported by a grant from the [Decentralized Wireless Alliance](https://dewi.org).
