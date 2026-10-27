# fractic-aws-s3

Wrapper over the AWS S3 SDK: `S3Util` (typed get / put of strings and
objects, uploads, presigned URLs), `S3CtxView`, typed errors, constants.
`src/lib.rs` → `constants.rs`, `context.rs`, `errors.rs`, `util.rs`
(+ `util/backend.rs` for the SDK calls).

- Keep the shape: errors in `errors.rs`, context requirements in
  `context.rs`, the public surface `util.rs` (`S3Util`) plus `pub use`s in
  `lib.rs`; `backend.rs` is the only place that calls the SDK. A new
  capability is a method on the util or an `ext/` module.
- Commit / PR titles: `[util] …`, `[ext] …`.
