load("@diff.bzl//diff:defs.bzl", "diff")
load("@protobuf//bazel:proto_library.bzl", "proto_library")

def vendored_bazel_protobuf_library(name, deps = []):
    proto_file_name = name.removesuffix("_proto")
    diff(
        name = "{}_patch".format(name),
        srcs = [
            "{}.proto".format(proto_file_name),
            "@bazel__{}//file".format(name),
        ],
        validate = 1,
    )

    proto_library(
        name = name,
        srcs = [
            "{}.proto".format(proto_file_name),
        ],
        deps = deps,
        strip_import_prefix = "/third_party/bazel",
        visibility = ["//visibility:public"],
    )
