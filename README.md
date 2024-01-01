# TP 1 - Banco de Dados

---



## Dependencies needed

[Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/)

If you are using Linux (Debian based)

```shell
$ sudo apt install docker docker-compose
```

## Running Docker

Once the dependencies are installed, you can run

```shell
$ make run-all
```

This command will download the ***amazon-meta*** file, populate the database and run the queries.

Or you can do each step individually

```shell
$ make download-amazon-meta # to download the amazon-meta file
$ make build # to build the docker compose based on the docker-compose.yml file
$ make run # to run the container in background
$ make populate-database # to read and populate the database
$ make run-queries # to run the queries over the database
```

Furthermore, you can also enter the containers

To enter the *PostgresSQL* container

```shell
$ docker exec -it tp1_aldemir_nilson_victor_postgres psql -U postgres
```

To enter the Python container

```shell
$ docker exec -it tp1_aldemir_nilson_victor_python bash
```



## Authors

---

[Aldemir Rodrigues da Silva](aldemir.silva@icomp.ufam.edu.br)<br>
[Nilson Andrade dos Santos Junior](nilson.andrade@icomp.ufam.edu.br)<br>
[Victor Hugo de Oliveira Gomes](victor.gomes@icomp.ufam.edu.br)<br>