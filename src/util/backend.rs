use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::{
    client::Waiters,
    error::SdkError,
    operation::{
        copy_object::{CopyObjectError, CopyObjectOutput},
        delete_object::{DeleteObjectError, DeleteObjectOutput},
        get_object::{GetObjectError, GetObjectOutput},
        head_object::{HeadObjectError, HeadObjectOutput},
        list_objects_v2::ListObjectsV2Error,
        put_object::{PutObjectError, PutObjectOutput},
    },
    presigning::{PresignedRequest, PresigningConfig},
    primitives::ByteStream,
    waiters::object_exists::{ObjectExistsFinalPoll, WaitUntilObjectExistsError},
};
use fractic_context::register_ctx_singleton;

use crate::S3CtxView;

// Underlying backend, which performs the actual AWS operations. Kept generic so
// that it can be swapped with a mock backend for testing.
//
// Should be kept as minimal and close as possible to the real
// aws_sdk_s3::Client, to minimize untestable code.
// #[automock] TODO
#[async_trait]
pub trait S3Backend: Send + Sync {
    async fn put_object(
        &self,
        bucket: String,
        key: String,
        body: ByteStream,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<PutObjectOutput, SdkError<PutObjectError>>;

    async fn get_object(
        &self,
        bucket: String,
        key: String,
    ) -> Result<GetObjectOutput, SdkError<GetObjectError>>;

    async fn head_object(
        &self,
        bucket: String,
        key: String,
    ) -> Result<HeadObjectOutput, SdkError<HeadObjectError>>;

    async fn copy_object(
        &self,
        bucket: String,
        source_key: String,
        target_key: String,
    ) -> Result<CopyObjectOutput, SdkError<CopyObjectError>>;

    async fn delete_object(
        &self,
        bucket: String,
        key: String,
    ) -> Result<DeleteObjectOutput, SdkError<DeleteObjectError>>;

    async fn generate_presigned_url(
        &self,
        bucket: String,
        key: String,
        expires_in: Duration,
    ) -> Result<PresignedRequest, SdkError<GetObjectError>>;

    async fn list_keys(
        &self,
        bucket: String,
        prefix: String,
    ) -> Result<Vec<String>, SdkError<ListObjectsV2Error>>;

    async fn wait_until_object_exists(
        &self,
        bucket: String,
        key: String,
        timeout: Duration,
    ) -> Result<ObjectExistsFinalPoll, WaitUntilObjectExistsError>;
}

// Real implementation,
// making actual calls to AWS.
// --------------------------------------------------

#[async_trait]
impl S3Backend for aws_sdk_s3::Client {
    async fn put_object(
        &self,
        bucket: String,
        key: String,
        body: ByteStream,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<PutObjectOutput, SdkError<PutObjectError>> {
        self.put_object()
            .bucket(bucket)
            .key(key)
            .body(body)
            .set_metadata(metadata)
            .send()
            .await
    }

    async fn get_object(
        &self,
        bucket: String,
        key: String,
    ) -> Result<GetObjectOutput, SdkError<GetObjectError>> {
        self.get_object().bucket(bucket).key(key).send().await
    }

    async fn head_object(
        &self,
        bucket: String,
        key: String,
    ) -> Result<HeadObjectOutput, SdkError<HeadObjectError>> {
        self.head_object().bucket(bucket).key(key).send().await
    }

    async fn copy_object(
        &self,
        bucket: String,
        source_key: String,
        target_key: String,
    ) -> Result<CopyObjectOutput, SdkError<CopyObjectError>> {
        self.copy_object()
            .copy_source(format!("{}/{}", bucket, source_key))
            .bucket(bucket)
            .key(target_key)
            .send()
            .await
    }

    async fn delete_object(
        &self,
        bucket: String,
        key: String,
    ) -> Result<DeleteObjectOutput, SdkError<DeleteObjectError>> {
        self.delete_object().bucket(bucket).key(key).send().await
    }

    async fn generate_presigned_url(
        &self,
        bucket: String,
        key: String,
        expires_in: Duration,
    ) -> Result<PresignedRequest, SdkError<GetObjectError>> {
        self.get_object()
            .bucket(bucket)
            .key(key)
            .presigned(PresigningConfig::expires_in(expires_in).unwrap())
            .await
    }

    async fn wait_until_object_exists(
        &self,
        bucket: String,
        key: String,
        timeout: Duration,
    ) -> Result<ObjectExistsFinalPoll, WaitUntilObjectExistsError> {
        Waiters::wait_until_object_exists(self)
            .bucket(bucket)
            .key(key)
            .wait(timeout)
            .await
    }

    async fn list_keys(
        &self,
        bucket: String,
        prefix: String,
    ) -> Result<Vec<String>, SdkError<ListObjectsV2Error>> {
        let mut keys = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut request = self
                .list_objects_v2()
                .bucket(bucket.clone())
                .prefix(prefix.clone());

            if let Some(token) = &continuation_token {
                request = request.continuation_token(token);
            }

            let response = request.send().await?;

            let objects = response.contents();
            keys.extend(
                objects
                    .iter()
                    .filter_map(|obj| obj.key().map(|k| k.to_string())),
            );

            // Check if the response is truncated; if so, continue with the next token.
            if response.is_truncated().unwrap_or(false) {
                if let Some(next_token) = response.next_continuation_token().map(|s| s.to_string())
                {
                    continuation_token = Some(next_token);
                    continue;
                }
            }

            break;
        }

        Ok(keys)
    }
}

// Register dependency, default to real AWS backend.
// --------------------------------------------------

register_ctx_singleton!(
    dyn S3CtxView,
    dyn S3Backend,
    |ctx: Arc<dyn S3CtxView>| async move {
        let region = Region::new(ctx.s3_region().clone());
        let shared_config = aws_config::defaults(BehaviorVersion::v2025_01_17())
            .region(region)
            .load()
            .await;
        Ok(aws_sdk_s3::Client::new(&shared_config))
    }
);
