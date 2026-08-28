//! The engine each client reports.

use std::sync::Arc;

use archivindex_http_client::recorder::Recorder;
use archivindex_http_client::reqwest::ReqwestClient;
use archivindex_http_client::{Client, Engine};
use cargo_metadata::semver::{Op, VersionReq};
use cargo_metadata::{DependencyKind, MetadataCommand};

/// Read the dependency requirement that downstream builds must satisfy, including inheritance.
fn dependency_requirement(name: &str) -> VersionReq {
    let metadata = MetadataCommand::new()
        .manifest_path(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
        .no_deps()
        .other_options(vec!["--offline".to_owned(), "--locked".to_owned()])
        .exec()
        .expect("readable package metadata");
    metadata
        .packages
        .iter()
        .find(|package| package.name == env!("CARGO_PKG_NAME"))
        .expect("this package is in the workspace")
        .dependencies
        .iter()
        .find(|dependency| dependency.name == name && dependency.kind == DependencyKind::Normal)
        .expect("a direct engine dependency")
        .req
        .clone()
}

/// Each client names its own engine, including when it is shared as a trait object.
#[test]
fn clients_report_their_engines() {
    let recorder: Arc<dyn Client> = Arc::new(Recorder::new());
    let reqwest: Arc<dyn Client> = Arc::new(ReqwestClient::new());

    assert_eq!(recorder.engine(), Engine::RECORDER);
    assert_eq!(reqwest.engine(), Engine::REQWEST);
}

/// An engine is displayed in the `User-Agent` product syntax, in which the version and the comment
/// holding the profile are each optional. The engines here are made up so that the expected text
/// does not change with a dependency's version.
#[test]
fn engines_are_displayed_as_products() {
    let engine = Engine {
        name: "example",
        version: None,
        profile: None,
    };
    let versioned = Engine {
        version: Some("1.2.3"),
        ..engine
    };
    let series = Engine {
        version: Some("1.2.x"),
        ..engine
    };
    let profiled = Engine {
        profile: Some("chrome_136"),
        ..engine
    };
    let complete = Engine {
        profile: Some("chrome_136"),
        ..versioned
    };

    assert_eq!(engine.to_string(), "example");
    assert_eq!(versioned.to_string(), "example/1.2.3");
    assert_eq!(series.to_string(), "example/1.2.x");
    assert_eq!(profiled.to_string(), "example (chrome_136)");
    assert_eq!(complete.to_string(), "example/1.2.3 (chrome_136)");
}

/// A single caret requirement on 0.minor.patch, with a nonzero minor, permits changes only within
/// that minor series. Check the requirement rather than one workspace's resolved patch version.
#[test]
fn reqwest_reports_the_permitted_release_series() {
    let requirement = dependency_requirement("reqwest");
    let [constraint] = requirement.comparators.as_slice() else {
        panic!("expected a single caret requirement, got {requirement}");
    };
    assert_eq!(constraint.op, Op::Caret);
    assert_eq!(constraint.major, 0);
    let minor = constraint.minor.expect("a minor version");
    assert!(minor > 0);
    assert!(constraint.pre.is_empty(), "expected a stable release");
    let series = format!("0.{minor}.x");
    assert_eq!(Engine::REQWEST.version, Some(series.as_str()));
}

/// The `wreq` engine carries the client's profile and the exact version its dependency requires.
#[cfg(feature = "wreq")]
#[test]
fn the_wreq_engine_has_its_profile_and_pinned_version() {
    use archivindex_http_client::wreq::WreqClient;
    use wreq_util::Profile;

    let engine = WreqClient::new(Profile::Chrome136).engine();

    assert_eq!(engine, Engine::wreq_with_profile(Profile::Chrome136));
    assert_eq!(engine.name, "wreq");
    assert_eq!(engine.profile, Some("chrome_136"));
    let version = engine.version.expect("a versioned engine");
    assert_eq!(
        dependency_requirement("wreq"),
        VersionReq::parse(&format!("={version}")).expect("an exact version requirement")
    );
}
