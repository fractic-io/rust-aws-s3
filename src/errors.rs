use fractic_generic_server_error::{
    define_internal_error_type, define_user_visible_error_type, GenericServerError,
    GenericServerErrorTrait,
};

define_user_visible_error_type!(S3NotFound, "Requested item does not exist.");
define_internal_error_type!(S3ConnectionError, "Generic S3 error.");
define_internal_error_type!(S3ItemParsingError, "S3 item parsing error.");
define_internal_error_type!(S3InvalidOperation, "Invalid S3 operation.");
