# fractic-aws-s3

Wrapper over the AWS S3 SDK: `S3Util` (typed get / put of strings and
objects, uploads, presigned URLs), `S3CtxView`, typed errors, constants.
`src/lib.rs` → `constants.rs`, `context.rs`, `errors.rs`, `util.rs`
(+ `util/backend.rs` for the SDK calls).
Consumer side: `/efs/fractic/_/rust.md` §2.
