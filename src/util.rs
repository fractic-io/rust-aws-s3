use std::{collections::HashMap, path::Path};

use aws_sdk_s3::{
    error::SdkError, operation::head_object::HeadObjectError, primitives::ByteStream,
};
use backend::S3BackendImpl;
use fractic_generic_server_error::{cxt, GenericServerError};

use crate::errors::S3InvalidOperation;

use super::errors::S3ConnectionError;

pub mod backend;

const WAIT_FOR_KEY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

pub struct S3Util<B: S3BackendImpl> {
    pub backend: B,
    pub bucket: String,
}
impl<'a, C: S3BackendImpl> S3Util<C> {
    pub async fn upload_file(
        &self,
        key: String,
        filename: &str,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<(), GenericServerError> {
        cxt!("S3Util::upload_file");
        let body = ByteStream::from_path(Path::new(filename))
            .await
            .map_err(|e| {
                S3InvalidOperation::with_debug(CXT, "Failed to open file.", format!("{:?}", e))
            })?;
        self.backend
            .put_object(self.bucket.clone(), key, body, metadata)
            .await
            .map_err(|e| {
                S3ConnectionError::with_debug(CXT, "Failed to upload file.", format!("{:?}", e))
            })?;
        Ok(())
    }

    pub async fn move_object(
        &self,
        source_key: String,
        target_key: String,
    ) -> Result<(), GenericServerError> {
        cxt!("S3Util::move_object");
        self.backend
            .copy_object(self.bucket.clone(), source_key.clone(), target_key)
            .await
            .map_err(|e| {
                S3ConnectionError::with_debug(CXT, "Failed to copy object.", format!("{:?}", e))
            })?;
        self.backend
            .delete_object(self.bucket.clone(), source_key)
            .await
            .map_err(|e| {
                S3ConnectionError::with_debug(CXT, "Failed to delete object.", format!("{:?}", e))
            })?;
        Ok(())
    }

    pub async fn key_exists(&self, key: String) -> Result<bool, GenericServerError> {
        cxt!("S3Util::key_exists");
        match self.backend.head_object(self.bucket.clone(), key).await {
            Ok(_) => Ok(true),
            Err(sdk_error) => match sdk_error {
                SdkError::ServiceError(e) => match e.err() {
                    HeadObjectError::NotFound(_) => Ok(false),
                    _ => Err(S3ConnectionError::with_debug(
                        CXT,
                        "Unexpected error running HeadObject operation.",
                        e.err().to_string(),
                    )),
                },
                _ => Err(S3ConnectionError::with_debug(
                    CXT,
                    "Failed to check key existance.",
                    format!("{:?}", sdk_error),
                )),
            },
        }
    }

    // If key exists, returns Some(metadata), else None.
    pub async fn get_metadata_if_key_exists(
        &self,
        key: String,
    ) -> Result<Option<HashMap<String, String>>, GenericServerError> {
        cxt!("S3Util::key_exists");
        match self.backend.head_object(self.bucket.clone(), key).await {
            Ok(output) => Ok(Some(output.metadata.unwrap_or_default())),
            Err(sdk_error) => match sdk_error {
                SdkError::ServiceError(e) => match e.err() {
                    HeadObjectError::NotFound(_) => Ok(None),
                    _ => Err(S3ConnectionError::with_debug(
                        CXT,
                        "Unexpected error running HeadObject operation.",
                        e.err().to_string(),
                    )),
                },
                _ => Err(S3ConnectionError::with_debug(
                    CXT,
                    "Failed to check key existance.",
                    format!("{:?}", sdk_error),
                )),
            },
        }
    }

    pub async fn wait_until_key_exists(&self, key: String) -> Result<(), GenericServerError> {
        cxt!("S3Util::wait_until_key_exists");
        self.backend
            .wait_until_object_exists(self.bucket.clone(), key, WAIT_FOR_KEY_TIMEOUT)
            .await
            .map_err(|e| {
                S3ConnectionError::with_debug(
                    CXT,
                    "Failed to wait for key existance.",
                    format!("{:?}", e),
                )
            })?;
        Ok(())
    }

    pub async fn generate_presigned_url(
        &self,
        key: String,
        expires_in: std::time::Duration,
    ) -> Result<String, GenericServerError> {
        cxt!("S3Util::generate_presigned_url");
        let presigned_request = self
            .backend
            .generate_presigned_url(self.bucket.clone(), key, expires_in)
            .await
            .map_err(|e| {
                S3ConnectionError::with_debug(
                    CXT,
                    "Failed to generate presigned URL.",
                    format!("{:?}", e),
                )
            })?;
        Ok(presigned_request.uri().into())
    }

    pub async fn get_size(&self, key: String) -> Result<i64, GenericServerError> {
        cxt!("S3Util::get_size");
        let output = self
            .backend
            .head_object(self.bucket.clone(), key)
            .await
            .map_err(|e| {
                S3ConnectionError::with_debug(CXT, "Failed to get object size.", format!("{:?}", e))
            })?;
        Ok(output.content_length.unwrap_or_default())
    }
}
