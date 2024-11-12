use fractic_server_error::{define_client_error, define_internal_error, define_user_error};

define_user_error!(S3NotFound, "Requested item does not exist.");
define_internal_error!(S3CalloutError, "S3 callout error: {details}.", { details: &str });
define_internal_error!(S3ItemParsingError, "S3 item parsing error: {details}.", { details: &str });
define_client_error!(S3InvalidOperation, "Invalid S3 operation: {details}.", { details: &str });
