pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Incomplete download. Invalid chunk checksum.")]
    CacheChunkChecksum,

    #[error("Cache service responded with {0} during upload chunk.")]
    CacheChunkUpload(http::StatusCode),

    #[error(
        "Incomplete download. Expected chunk size: {expected_size}, actual chunk size: {actual_size}"
    )]
    CacheChunkDownload {
        expected_size: usize,
        actual_size: usize,
    },

    #[error("Cache service responded with {0} during commit cache.")]
    CacheCommit(http::StatusCode),

    #[error(
        "Incomplete download. Expected file size: {expected_size}, actual file size: {actual_size}"
    )]
    CacheDownload {
        expected_size: usize,
        actual_size: usize,
    },

    #[error("Cache not found.")]
    CacheNotFound,

    #[error("Cache service responded with {0}")]
    CacheServiceStatus(http::StatusCode),

    #[error("Cache size of {0} bytes is too large.")]
    CacheSize(usize),

    #[error(transparent)]
    InvalidHeaderValue(#[from] http::header::InvalidHeaderValue),

    #[error("Key Validation Error: {0} cannot contain commas.")]
    InvalidKeyComma(String),

    #[error("Key Validation Error: {0} cannot be larger than 512 characters.")]
    InvalidKeyLength(String),

    #[error(transparent)]
    IO(#[from] std::io::Error),

    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    #[error(transparent)]
    ReqwestMiddleware(#[from] reqwest_middleware::Error),

    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    #[error(transparent)]
    SerdeUrlencodedSerialize(#[from] serde_urlencoded::ser::Error),

    #[error(transparent)]
    UrlParse(#[from] url::ParseError),
}
