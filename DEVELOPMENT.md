# Development

## Remove red squiggles on generated prost crates

Run the following to generate a rust-project.json in your project root. This will tell rust-analyzer about crates in the Bazel output tree.

```bash
bazel run @rules_rust//tools/rust_analyzer:gen_rust_project
```

