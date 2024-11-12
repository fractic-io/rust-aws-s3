use std::{collections::HashMap, path::Path};

use aws_sdk_s3::{
    error::SdkError,
    operation::head_object::HeadObjectError,
    primitives::{ByteStream, SdkBody},
};
use backend::S3BackendImpl;
use fractic_server_error::ServerError;
use serde::Serialize;

use crate::errors::{S3CalloutError, S3InvalidOperation, S3ItemParsingError, S3NotFound};

pub mod backend;

const WAIT_FOR_KEY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

pub struct S3Util<B: S3BackendImpl> {
    pub backend: B,
    pub bucket: String,
}

pub struct S3KeyGenerator {}
impl S3KeyGenerator {
    pub fn date_partitioned_unique_key(
        prefix: &str,
        datetime: &chrono::DateTime<chrono::Utc>,
    ) -> String {
        format!(
            "{prefix}/{year}/{month}/{day}/{epoch:011}-{uuid}",
            year = datetime.format("%Y"),
            month = datetime.format("%m"),
            day = datetime.format("%d"),
            epoch = datetime.timestamp(),
            uuid = uuid::Uuid::new_v4(),
        )
    }
}

impl<'a, C: S3BackendImpl> S3Util<C> {
    pub async fn put_serializable<T: Serialize>(
        &self,
        key: String,
        data: T,
    ) -> Result<(), ServerError> {
        let serialized = serde_json::to_string(&data)
            .map_err(|e| S3InvalidOperation::with_debug("Failed to serialize object.", &e))?;
        let body = ByteStream::new(SdkBody::from(serialized));
        self.backend
            .put_object(self.bucket.clone(), key, body, None)
            .await
            .map_err(|e| S3CalloutError::with_debug("Failed to put serializable.", &e))?;
        Ok(())
    }

    pub async fn get_serializable<T: for<'de> serde::Deserialize<'de>>(
        &self,
        key: String,
    ) -> Result<T, ServerError> {
        let output = self
            .backend
            .get_object(self.bucket.clone(), key)
            .await
            .map_err(|_| S3NotFound::new())?;
        let bytes = output
            .body
            .collect()
            .await
            .map_err(|e| S3CalloutError::with_debug("Failed to read object body.", &e))?
            .into_bytes();
        let deserialized = serde_json::from_slice(&bytes)
            .map_err(|e| S3ItemParsingError::with_debug("Failed to deserialize object.", &e))?;
        Ok(deserialized)
    }

    pub async fn upload_file(
        &self,
        key: String,
        filename: &str,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<(), ServerError> {
        let body = ByteStream::from_path(Path::new(filename))
            .await
            .map_err(|e| S3InvalidOperation::with_debug("Failed to open file.", &e))?;
        self.backend
            .put_object(self.bucket.clone(), key, body, metadata)
            .await
            .map_err(|e| S3CalloutError::with_debug("Failed to upload file.", &e))?;
        Ok(())
    }

    pub async fn move_object(
        &self,
        source_key: String,
        target_key: String,
    ) -> Result<(), ServerError> {
        self.backend
            .copy_object(self.bucket.clone(), source_key.clone(), target_key)
            .await
            .map_err(|e| S3CalloutError::with_debug("Failed to copy object.", &e))?;
        self.backend
            .delete_object(self.bucket.clone(), source_key)
            .await
            .map_err(|e| S3CalloutError::with_debug("Failed to delete object.", &e))?;
        Ok(())
    }

    pub async fn delete_object(&self, key: String) -> Result<(), ServerError> {
        self.backend
            .delete_object(self.bucket.clone(), key)
            .await
            .map_err(|e| S3CalloutError::with_debug("Failed to delete object.", &e))?;
        Ok(())
    }

    pub async fn key_exists(&self, key: String) -> Result<bool, ServerError> {
        match self.backend.head_object(self.bucket.clone(), key).await {
            Ok(_) => Ok(true),
            Err(sdk_error) => match sdk_error {
                SdkError::ServiceError(e) => match e.err() {
                    HeadObjectError::NotFound(_) => Ok(false),
                    _ => Err(S3CalloutError::with_debug(
                        "Unexpected error running HeadObject operation.",
                        &e.err().to_string(),
                    )),
                },
                _ => Err(S3CalloutError::with_debug(
                    "Failed to check key existance.",
                    &sdk_error,
                )),
            },
        }
    }

    // If key exists, returns Some(metadata), else None.
    pub async fn get_metadata_if_key_exists(
        &self,
        key: String,
    ) -> Result<Option<HashMap<String, String>>, ServerError> {
        match self.backend.head_object(self.bucket.clone(), key).await {
            Ok(output) => Ok(Some(output.metadata.unwrap_or_default())),
            Err(sdk_error) => match sdk_error {
                SdkError::ServiceError(e) => match e.err() {
                    HeadObjectError::NotFound(_) => Ok(None),
                    _ => Err(S3CalloutError::with_debug(
                        "Unexpected error running HeadObject operation.",
                        &e.err().to_string(),
                    )),
                },
                _ => Err(S3CalloutError::with_debug(
                    "Failed to check key existance.",
                    &sdk_error,
                )),
            },
        }
    }

    pub async fn wait_until_key_exists(&self, key: String) -> Result<(), ServerError> {
        self.backend
            .wait_until_object_exists(self.bucket.clone(), key, WAIT_FOR_KEY_TIMEOUT)
            .await
            .map_err(|e| S3CalloutError::with_debug("Failed to wait for key existance.", &e))?;
        Ok(())
    }

    pub async fn generate_presigned_url(
        &self,
        key: String,
        expires_in: std::time::Duration,
    ) -> Result<String, ServerError> {
        let presigned_request = self
            .backend
            .generate_presigned_url(self.bucket.clone(), key, expires_in)
            .await
            .map_err(|e| S3CalloutError::with_debug("Failed to generate presigned URL.", &e))?;
        Ok(presigned_request.uri().into())
    }

    pub async fn get_size(&self, key: String) -> Result<i64, ServerError> {
        let output = self
            .backend
            .head_object(self.bucket.clone(), key)
            .await
            .map_err(|e| S3CalloutError::with_debug("Failed to get object size.", &e))?;
        Ok(output.content_length.unwrap_or_default())
    }
}
