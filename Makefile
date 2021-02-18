start-db:
	docker run --rm --name reddit-postgres -v $(shell pwd)/reddit_db:/var/lib/postgresql/data \
	 -p 5442:5432 -e POSTGRES_PASSWORD=mysecretpassword -d postgres:13
	docker exec reddit-postgres sh -c 'until pg_isready; do echo "Waiting for the DB to be up..."; sleep 4; done'
	docker exec reddit-postgres sh -c "echo 'CREATE DATABASE reddit;' |psql -U postgres"

tabula-rasa:
	docker kill reddit-postgres || true
	docker rm reddit-postgres || true
	rm -rf $(shell pwd)/reddit_db
