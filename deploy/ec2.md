# Running Seatline Lakehouse on the EC2 box

The machine is the same c7i-flex.large (2 vCPU, 4 GB RAM, 4 GB swap) that
serves judgebox, Seatline and the grid dashboard. Seatline Lakehouse needs most of its
memory, so the two app stacks are stopped while it runs. The grid dashboard
is static files behind Caddy and can stay up.

Lines starting with `$` run on the server (`ubuntu@...` prompt).

## 1. Make room

```
$ df -h /
$ free -h
```

The images (Spark, Airflow with Java, Kafka, Debezium, MinIO, Postgres) need
about 6 GB of disk. If `df` shows less than 8 GB free, grow the EBS volume in
the console (EC2, Volumes, Modify; 30 GB stays inside the free tier), then:

```
$ lsblk          # find the root disk and partition names
$ sudo growpart /dev/nvme0n1 1 && sudo resize2fs /dev/nvme0n1p1
```

Stop the two app stacks. Their data volumes are kept.

```
$ cd ~/judgebox && docker compose stop
$ cd ~/seatline && docker compose stop
```

## 2. Get the code and set secrets

```
$ cd ~ && git clone https://github.com/Mahendr99ar/seatline-lakehouse.git && cd seatline-lakehouse
$ cp .env.example .env
$ for k in SOURCE_PASSWORD DEBEZIUM_PASSWORD CATALOG_PASSWORD MINIO_PASSWORD AIRFLOW_ADMIN_PASSWORD; do
    sed -i "s/^$k=.*/$k=$(openssl rand -hex 16)/" .env; done
$ sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env
$ chmod 600 .env
```

## 3. Start it

```
$ docker compose up -d --build
$ docker compose ps
```

The first start downloads the Iceberg, Kafka and Postgres JDBC jars into the
`ivy-*` volumes, so `spark-stream` takes a few minutes before its first batch.
Follow it with:

```
$ docker compose logs -f spark-stream
$ docker compose logs connect-init
```

`connect-init` should print the connector status with `"state":"RUNNING"`.

## 4. Check the lake matches the source

```
$ ./scripts/check.sh
```

Source and silver row counts should agree to within the last minute of
changes. Run it twice a minute apart if they look off.

## 5. Airflow

The UI listens on 127.0.0.1 only. From the laptop:

```
ssh -i portfolio.pem -L 8090:127.0.0.1:8090 ubuntu@52.62.146.64
```

then open http://localhost:8090 and log in as `admin` with
`AIRFLOW_ADMIN_PASSWORD` from `.env` (`grep AIRFLOW_ADMIN ~/seatline-lakehouse/.env`
on the server; do not paste it anywhere). Turn on `lakehouse_gold` and
`lakehouse_maintenance`. The first gold run writes `dashboard/data.json`.

## 6. Publish the dashboard

Create a DuckDNS subdomain (for example `lake-mahendra`) pointing at the
Elastic IP. Then add a site to `~/Caddyfile`:

```
lake-mahendra.duckdns.org {
    root * /srv/lakehouse
    file_server
}
```

Caddy needs the folder mounted. Check how the container was started before
replacing it:

```
$ docker inspect caddy --format '{{range .Mounts}}{{.Source}} -> {{.Destination}}{{"\n"}}{{end}}'
```

and recreate it with the same mounts plus the new one:

```
$ docker rm -f caddy
$ docker run -d --name caddy --network host --restart unless-stopped \
    -v ~/Caddyfile:/etc/caddy/Caddyfile:ro \
    -v caddy_data:/data \
    -v ~/india-grid-warehouse/dashboard:/srv/grid:ro \
    -v ~/seatline-lakehouse/dashboard:/srv/lakehouse:ro \
    caddy:2
```

## 7. Measure, then put the apps back

While it runs, note the numbers the README is waiting for:

```
$ docker stats --no-stream
$ docker compose logs spark-stream | grep "^batch" | tail
```

To stop Seatline Lakehouse and bring the apps back:

```
$ cd ~/seatline-lakehouse && docker compose stop
$ cd ~/judgebox && docker compose start
$ cd ~/seatline && docker compose start
```

The last `data.json` stays on disk, so the dashboard keeps working after the
pipeline is stopped.
