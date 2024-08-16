POSTGRES_CONTAINER_NAME=pgboss
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5444
POSTGRES_USER=pgboss_user
POSTGRES_PASSWORD=pgboss_password
POSTGRES_DATABASE=pgboss

.PHONY: precommit
precommit: fmt check test/doc test/e2e

.PHONY: fmt
fmt:
	cargo fmt

.PHONY: check
check:
	cargo fmt --check
	cargo clippy --all-features
	cargo d --no-deps --all-features

.PHONY: doc
doc:
	RUSTDOCFLAGS='--cfg docsrs' cargo +nightly d --all-features --open

.PHONY: postgres
postgres:
	docker ps | grep -i ${POSTGRES_CONTAINER_NAME} || \
	docker run -d --name ${POSTGRES_CONTAINER_NAME} \
	-p ${POSTGRES_HOST}:${POSTGRES_PORT}:5432 \
	-e POSTGRES_USER=${POSTGRES_USER} \
	-e POSTGRES_PASSWORD=${POSTGRES_PASSWORD} \
	-e POSTGRES_DB=${POSTGRES_DATABASE} \
	postgres
	sleep 15

.PHONY: postgres/start
postgres/start:
	docker start ${POSTGRES_CONTAINER_NAME}

.PHONY: postgres/psql
postgres/psql:
	docker exec -it pgboss sh -c "psql -U pgboss_user pgboss"

.PHONY: postgres/kill
postgres/kill:
	docker stop ${POSTGRES_CONTAINER_NAME} && docker rm ${POSTGRES_CONTAINER_NAME}

.PHONY: test/doc
test/doc:
	cargo test --locked --all-features --doc

.PHONY: test/e2e
test/e2e:
	POSTGRES_URL=postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DATABASE} \
	cargo test --locked --all-features --all-targets --test e2e -- --nocapture

.PHONY: test
test: test/e2e
