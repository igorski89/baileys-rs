//! Environment variables, with the TypeScript connector's names and defaults.
use std::path::{Path, PathBuf};

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

/// Where the session lives: a SQLite file, or memory only (`:memory:`).
#[derive(Debug, Clone, PartialEq)]
pub enum SessionDb {
    Memory,
    File(PathBuf),
}

impl SessionDb {
    pub fn parse(text: &str) -> Self {
        if text == ":memory:" {
            Self::Memory
        } else {
            Self::File(text.into())
        }
    }

    /// The database's folder, where a TypeScript connector's session files
    /// are imported from; none for an in-memory session.
    pub fn folder(&self) -> Option<&Path> {
        match self {
            Self::Memory => None,
            Self::File(path) => Some(
                path.parent()
                    .filter(|f| !f.as_os_str().is_empty())
                    .unwrap_or(Path::new(".")),
            ),
        }
    }
}

impl std::fmt::Display for SessionDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Memory => f.write_str(":memory:"),
            Self::File(path) => write!(f, "{}", path.display()),
        }
    }
}

pub const USAGE: &str = "Usage: baileys-sqs [--session-db <path|:memory:>]

Runs the connector with the settings in the environment (see .env.example).
--session-db overrides SESSION_DB: the session's SQLite file, or :memory:
for a session that is never written to disk.";

/// The session database from `--session-db`, then `SESSION_DB`, then
/// `./auth.sqlite3`. `SESSION_DIR` alone is an error: it is no longer read,
/// and silently starting a new, unpaired session would hide that.
pub fn session_db(
    args: &[String],
    env_db: Option<String>,
    env_dir: Option<String>,
) -> Result<SessionDb, String> {
    let mut arg = None;
    let mut rest = args.iter();
    while let Some(a) = rest.next() {
        match a.split_once('=') {
            Some(("--session-db" | "--session_db", value)) => arg = Some(value.to_owned()),
            _ if a == "--session-db" || a == "--session_db" => {
                arg = Some(
                    rest.next()
                        .ok_or(format!("{a} needs a value\n\n{USAGE}"))?
                        .clone(),
                )
            }
            _ => return Err(format!("unknown argument {a}\n\n{USAGE}")),
        }
    }
    if let Some(db) = arg.filter(|d| !d.is_empty()).or(env_db) {
        return Ok(SessionDb::parse(&db));
    }
    if let Some(dir) = env_dir {
        let path = Path::new(&dir).join("auth.sqlite3");
        return Err(format!(
            "SESSION_DIR is no longer read; set SESSION_DB to the session's database file, e.g. SESSION_DB={}",
            path.display()
        ));
    }
    Ok(SessionDb::File("./auth.sqlite3".into()))
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub input_queue: Option<String>,
    pub output_queue: Option<String>,
    pub session_db: SessionDb,
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
    /// The environment with the command line's `--session-db`.
    pub fn from_env_and_args(args: &[String]) -> Result<Self, String> {
        let mut settings = Self::from_env();
        settings.session_db = session_db(args, var("SESSION_DB"), var("SESSION_DIR"))?;
        Ok(settings)
    }

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
            session_db: var("SESSION_DB")
                .map(|db| SessionDb::parse(&db))
                .unwrap_or(SessionDb::File("./auth.sqlite3".into())),
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

/// Map pino level names onto a tracing filter. As the pino logger covers
/// only the connector and Baileys, the level applies to the connector and
/// baileys-rs; other crates (the AWS SDK, HTTP and TLS) log warnings and
/// errors only, unless the level is stricter.
pub fn log_filter(level: &str) -> String {
    let level = match level {
        "fatal" => "error",
        "silent" => "off",
        other => other,
    };
    match level {
        "trace" | "debug" | "info" => format!("warn,baileys_sqs={level},baileys_rs={level}"),
        other => other.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::{SessionDb, parse_int, session_db};
    use std::path::Path;

    #[test]
    fn the_session_database_comes_from_the_flag_then_the_environment() {
        let args = |a: &[&str]| a.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let file = |p: &str| SessionDb::File(p.into());
        assert_eq!(session_db(&[], None, None), Ok(file("./auth.sqlite3")));
        assert_eq!(
            session_db(&[], Some("/s/a.db".into()), None),
            Ok(file("/s/a.db"))
        );
        assert_eq!(
            session_db(
                &args(&["--session-db", ":memory:"]),
                Some("/s/a.db".into()),
                None
            ),
            Ok(SessionDb::Memory)
        );
        assert_eq!(
            session_db(&args(&["--session_db=/x.db"]), None, Some("/s".into())),
            Ok(file("/x.db"))
        );
        assert_eq!(
            session_db(&[], None, Some("/session".into())),
            Err("SESSION_DIR is no longer read; set SESSION_DB to the session's database file, e.g. SESSION_DB=/session/auth.sqlite3".into())
        );
        assert!(session_db(&args(&["--session-db"]), None, None).is_err());
        assert!(session_db(&args(&["--other"]), None, None).is_err());
        assert_eq!(
            file("/session/auth.sqlite3").folder(),
            Some(Path::new("/session"))
        );
        assert_eq!(file("auth.sqlite3").folder(), Some(Path::new(".")));
        assert_eq!(SessionDb::Memory.folder(), None);
    }

    #[test]
    fn log_levels_cover_the_connector_and_the_library() {
        use super::log_filter;
        assert_eq!(
            log_filter("debug"),
            "warn,baileys_sqs=debug,baileys_rs=debug"
        );
        assert_eq!(log_filter("error"), "error");
        assert_eq!(log_filter("fatal"), "error");
        assert_eq!(log_filter("silent"), "off");
    }

    #[test]
    fn parse_int_reads_a_decimal_prefix() {
        assert_eq!(parse_int(" 3000abc"), Some(3000));
        assert_eq!(parse_int("-5"), Some(-5));
        assert_eq!(parse_int("x"), None);
    }
}
