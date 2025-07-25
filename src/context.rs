use fractic_context::define_ctx_view;

define_ctx_view!(
    name: S3CtxView,
    env {
        S3_REGION: String,
    },
    secrets {},
    deps_overlay {
        dyn crate::util::backend::S3Backend,
    },
    req_impl {}
);
