use fractic_context::define_ctx_view;

define_ctx_view!(
    name: S3CtxView,
    env {
        S3_REGION: String,
    },
    secrets {},
    deps {},
    req_impl {}
);
