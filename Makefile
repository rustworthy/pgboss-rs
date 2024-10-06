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
	docker compose -f docker/compose.yaml up -d --build
	docker ps
	sleep 10
	docker compose -f docker/compose.yaml logs postgres --tail 10

.PHONY: postgres/start
postgres/start:
	docker start ${POSTGRES_CONTAINER_NAME}

.PHONY: postgres/psql
postgres/psql:
	docker exec -it pgboss sh -c "psql -U username pgboss"

.PHONY: postgres/kill
postgres/kill:
	docker compose -f docker/compose.yaml down -v

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
	cargo run --release --features binaries --bin loadtest -- $(args)
