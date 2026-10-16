//! Environment variables, with the TypeScript connector's names and defaults.
use std::path::PathBuf;

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// `parseInt` for a decimal prefix; `None` where JavaScript yields NaN.
fn parse_int(text: &str) -> Option<i64> {
    let text = text.trim_start_matches(crate::js::is_space);
    let (sign, digits) = match text.strip_prefix('-') {
        Some(rest) => (-1, rest),
        None => (1, text.strip_prefix('+').unwrap_or(text)),
    };
    let end = digits
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(digits.len());
    digits[..end].parse::<i64>().ok().map(|n| sign * n)
}

/// One AWS service's connection settings.
#[derive(Debug, Clone, Default)]
pub struct AwsService {
    pub region: String,
    pub endpoint_url: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
}

impl AwsService {
    /// A client config: explicit region, endpoint and static credentials when
    /// both keys are set; otherwise the SDK's default chain, as the JS SDK.
    pub async fn sdk_config(&self) -> aws_config::SdkConfig {
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(self.region.clone()));
        if let Some(endpoint) = &self.endpoint_url {
            loader = loader.endpoint_url(endpoint);
        }
        if let (Some(id), Some(secret)) = (&self.access_key_id, &self.secret_access_key) {
            loader = loader.credentials_provider(aws_credential_types::Credentials::new(
                id,
                secret,
                None,
                None,
                "environment",
            ));
        }
        loader.load().await
    }
}

#[derive(Debug, Clone)]
pub struct S3Settings {
    pub bucket: String,
    pub prefix: String,
    pub service: AwsService,
    pub public_url: Option<String>,
    pub force_path_style: bool,
    pub url_expiration_seconds: u64,
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub input_queue: Option<String>,
    pub output_queue: Option<String>,
    pub session_dir: PathBuf,
    pub use_pairing_code: bool,
    pub phone_number: Option<String>,
    pub whatsapp_version: Option<String>,
    pub socket_url: Option<String>,
    pub sqs: AwsService,
    pub s3: Option<S3Settings>,
    pub http_port: Option<u16>,
    pub http_host: String,
    pub auth_token: Option<String>,
    pub log_level: String,
}

impl Settings {
    pub fn from_env() -> Self {
        let aws_region = var("AWS_REGION");
        let aws_key = var("AWS_ACCESS_KEY_ID");
        let aws_secret = var("AWS_SECRET_ACCESS_KEY");
        let sqs = AwsService {
            region: var("SQS_REGION")
                .or(aws_region.clone())
                .unwrap_or_else(|| "us-east-1".into()),
            endpoint_url: var("SQS_ENDPOINT_URL").or_else(|| var("AWS_ENDPOINT_URL")),
            access_key_id: var("SQS_ACCESS_KEY_ID").or(aws_key.clone()),
            secret_access_key: var("SQS_SECRET_ACCESS_KEY").or(aws_secret.clone()),
        };
        let s3_endpoint = var("S3_ENDPOINT_URL");
        let s3 = var("S3_BUCKET").map(|bucket| S3Settings {
            bucket,
            prefix: {
                let prefix = var("S3_PREFIX").unwrap_or_else(|| "baileys-sqs/media".into());
                prefix.strip_suffix('/').unwrap_or(&prefix).to_owned()
            },
            force_path_style: var("S3_FORCE_PATH_STYLE").as_deref() == Some("true")
                || s3_endpoint.is_some(),
            public_url: var("S3_PUBLIC_URL"),
            url_expiration_seconds: var("S3_URL_EXPIRATION_SECONDS")
                .as_deref()
                .and_then(parse_int)
                .and_then(|n| u64::try_from(n).ok())
                .unwrap_or(604_800),
            service: AwsService {
                region: var("S3_REGION")
                    .or(aws_region)
                    .unwrap_or_else(|| "us-east-1".into()),
                endpoint_url: s3_endpoint,
                access_key_id: var("S3_ACCESS_KEY_ID").or(aws_key),
                secret_access_key: var("S3_SECRET_ACCESS_KEY").or(aws_secret),
            },
        });
        Self {
            input_queue: var("INPUT_QUEUE"),
            output_queue: var("OUTPUT_QUEUE"),
            session_dir: var("SESSION_DIR").unwrap_or_else(|| "./auth".into()).into(),
            use_pairing_code: var("USE_PAIRING_CODE").as_deref() == Some("true"),
            phone_number: var("PHONE_NUMBER"),
            whatsapp_version: var("WHATSAPP_VERSION"),
            socket_url: var("SOCKET_URL"),
            sqs,
            s3,
            http_port: var("PORT")
                .as_deref()
                .and_then(parse_int)
                .and_then(|n| u16::try_from(n).ok())
                .filter(|n| *n != 0),
            http_host: var("HTTP_HOST").unwrap_or_else(|| "0.0.0.0".into()),
            auth_token: var("AUTH_TOKEN"),
            log_level: var("LOG_LEVEL").unwrap_or_else(|| "debug".into()),
        }
    }
}

/// Map pino level names onto a tracing filter.
pub fn log_filter(level: &str) -> String {
    match level {
        "fatal" => "error".into(),
        "silent" => "off".into(),
        other => other.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_int;

    #[test]
    fn parse_int_reads_a_decimal_prefix() {
        assert_eq!(parse_int(" 3000abc"), Some(3000));
        assert_eq!(parse_int("-5"), Some(-5));
        assert_eq!(parse_int("x"), None);
    }
}
