.PHONY: test test-rust

test: test-rust

test-rust:
	cd redis-compat-rs && cargo test
