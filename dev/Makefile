configure:
	make clean
	cp -r CH-TEMPLATE clickhouse01
	cp -r CH-TEMPLATE clickhouse02
	cp -r CH-TEMPLATE clickhouse03
	cp -r CH-TEMPLATE clickhouse04
	REPLICA=01 SHARD=01 envsubst < CH-TEMPLATE/config.xml > clickhouse01/config.xml
	REPLICA=02 SHARD=01 envsubst < CH-TEMPLATE/config.xml > clickhouse02/config.xml
	REPLICA=03 SHARD=02 envsubst < CH-TEMPLATE/config.xml > clickhouse03/config.xml
	REPLICA=04 SHARD=02 envsubst < CH-TEMPLATE/config.xml > clickhouse04/config.xml

up:
	docker-compose up

up-deamon:
	docker-compose up -d

start:
	docker-compose start

down:
	docker-compose down -v
	docker volume prune -f

clean:
	rm -rf clickhouse01 clickhouse02 clickhouse03 clickhouse04