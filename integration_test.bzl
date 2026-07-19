"""Utilities for building rust integration tests"""

load(
    "@rules_rs//rs:rust_test.bzl",
    "rust_test",
)

def rust_integration_test(name, src, deps, **kwargs):
    """An integration rust_test target that includes a common shared module between test crates.

    This allows following the "submodules in integration tests" pattern outlined in the rust book,
    under Bazel.
    https://doc.rust-lang.org/book/ch11-03-test-organization.html#submodules-in-integration-tests

    A caveat is that the common test code is recompiled into each crate, which is not very Bazel
    idiomatic.

    Args:
        name: Name of the test target
        src: Test file to ues as the crate root
        deps: Additional depenendencies specific to the test crate
        **kwargs: Additional attributes to pass to the rust_test target
    """
    rust_test(
        name = name,
        srcs = [src, "tests/common/mod.rs"],
        crate_root = src,
        deps = list(set([
            ":lib",
            "//third_party/googleapis/google/devtools/build/v1:build_rust_proto",
            "@crates//:anyhow",
            "@crates//:futures",
            "@crates//:hyper-util",
            "@crates//:prost-types",
            "@crates//:rcgen",
            "@crates//:tempfile",
            "@crates//:tokio",
            "@crates//:tonic",
            "@crates//:tower",
            "@crates//:url",
            "@crates//:uuid",
        ] + deps)),
        **kwargs
    )
