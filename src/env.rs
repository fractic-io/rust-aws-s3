use fractic_env_config::{define_env_config, define_env_variable, EnvConfigEnum};

define_env_variable!(S3_REGION);

define_env_config!(
    S3EnvConfig,
    S3Region => S3_REGION,
);
