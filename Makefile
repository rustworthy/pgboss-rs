POSTGRES_CONTAINER_NAME=pgboss
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5444
POSTGRES_USER=username
POSTGRES_PASSWORD=password
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

# https://users.rust-lang.org/t/how-to-document-optional-features-in-api-docs/64577/3
.PHONY: doc
doc:
	RUSTDOCFLAGS='--cfg docsrs' cargo +nightly d --all-features --open

.PHONY: postgres
postgres:
	./docker/run-postgres.sh ${POSTGRES_HOST} ${POSTGRES_PORT}

.PHONY: postgres/start
postgres/start:
	docker start ${POSTGRES_CONTAINER_NAME}

.PHONY: postgres/logs
postgres/logs:
	docker logs -f ${POSTGRES_CONTAINER_NAME}

.PHONY: postgres/psql
postgres/psql:
	docker exec -it pgboss sh -c "psql -U username pgboss"

.PHONY: postgres/kill
postgres/kill:
	docker compose -f docker/compose.yaml down -v

# This will dump the entire schma description to `./docker/pgdump` directory
# mounted to the PostgreSQL docker container.
#
# We are running each end-to-end test against a dedicated schema (with the test
# function's name normally) and are leaving those schemas behind for debugging.
# With the next test run, they get cleaned up and re-created again. We are doing
# this for better isolation.
#
# We avoid calling any of the tests `pgboss` and we
# also avoid auto-creating this default schema (by not providing a schema name
# to `Client` when connecting, see end-to-end testcases). This is important, because
# we also run can the original `pgboss` example
# (https://github.com/timgit/pg-boss/blob/3da860f0e6f0650dcb95f62e5b71af6dfbeb44f1/examples/readme.mjs)
# against the same container. This example auto-creates `pgboss` schema.
#
# This way we can dump both any of our schemas and the original one, with;
# ```sh
# make postgres/dump schema=send_job
# make postgres/dump schema=pgboss
# ```
#
# And then see the diff:
# ```sh
# diff docker/pgdump/pgboss.sql docker/pgdump/send_job.sql --color
# ```
# Handy diff-viewer also: https://www.diffchecker.com/text-compare/
.PHONY: postgres/dump
postgres/dump:
	docker compose -f docker/compose.yaml exec postgres \
		sh -c "pg_dump -U username --schema $(schema) --schema-only pgboss > /var/lib/postgresql/pgdump/$(schema).sql"

.PHONY: postgres/dump/table
postgres/dump/table:
	docker compose -f docker/compose.yaml exec postgres \
		sh -c "pg_dump -U username --schema $(schema) --table $(table) --schema-only pgboss > /var/lib/postgresql/pgdump/$(schema)_$(table).sql"

.PHONY: test/doc
test/doc:
	cargo test --locked --all-features --doc

.PHONY: test
test: test/e2e

.PHONY: test/e2e
test/e2e:
	POSTGRES_URL=postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DATABASE} \
	cargo test --locked --all-features --all-targets --test e2e -- --nocapture --include-ignored $(pattern)

.PHONY: test/cov
test/cov:
	POSTGRES_URL=postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DATABASE} \
	cargo llvm-cov --locked --all-features --lcov --test e2e --output-path lcov.info -- --include-ignored

.PHONY: test/load
test/load:
	POSTGRES_URL=postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DATABASE} \
	cargo run --release --features binaries --bin loadtest -- $(args)
