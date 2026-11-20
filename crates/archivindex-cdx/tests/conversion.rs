//! Shared metadata must survive conversion from every supported CDX format.

use archivindex_cdx::format::{cdxj, classic, json};
use archivindex_cdx::{Entry, field};
use serde_json::{Value, json};

fn cdxj_entry(extra: Value) -> cdxj::Entry<'static> {
    let mut entry: cdxj::Entry<'static> =
        r#"com,example)/ 20201007212236 {"url":"https://example.com/"}"#
            .parse()
            .unwrap();
    let Value::Object(extra) = extra else {
        panic!("extension properties must be an object");
    };
    entry.fields.extra = extra.into();
    entry
}

#[test]
fn formats_agree_on_redirects_robot_flags_and_revisit_locations() {
    let header =
        classic::Header::parse("CDX N b a r M orig.length orig.offset orig.filename custom")
            .unwrap();
    let classic = header
        .parse_entry(
            "com,example)/ 20201007212236 https://example.com/ https://example.com/next A 1300 784 data.warc.gz retained",
        )
        .unwrap();
    let expected = header.decode(&classic).unwrap();
    let document: json::Document<'_> = serde_json::from_str(
        r#"[["urlkey","timestamp","original","redirect","robotflags","orig.length","orig.offset","orig.filename","custom"],
        ["com,example)/","20201007212236","https://example.com/","https://example.com/next","A","1300","784","data.warc.gz","retained"]]"#,
    )
    .unwrap();
    assert_eq!(
        document.into_entries().unwrap().entries,
        std::slice::from_ref(&expected)
    );

    for (length, offset) in [(json!(1300), json!("784")), (json!("1300"), json!(784))] {
        for flags in ["robotflags", "meta"] {
            let entry = cdxj_entry(json!({
                "redirect": "https://example.com/next",
                flags: "A",
                "orig.length": length,
                "orig.offset": offset,
                "orig.filename": "data.warc.gz",
                "custom": "retained"
            }));
            assert_eq!(Entry::try_from(entry).unwrap(), expected);
        }
    }
}

#[test]
fn absent_metadata_and_unknown_json_properties_are_preserved() {
    for absent in [Value::Null, json!("-")] {
        for length in [absent.clone(), json!(-1), json!("-123")] {
            let unknown = json!({"custom": {"nested": [1, true, null]}, "other": null});
            let mut entry = cdxj_entry(unknown.clone());
            entry.fields.extra.extend(
                json!({
                    "redirect": absent,
                    "robotflags": absent,
                    "orig.length": length,
                    "orig.offset": absent,
                    "orig.filename": absent
                })
                .as_object()
                .unwrap()
                .clone(),
            );
            let shared = Entry::try_from(entry).unwrap();
            assert_eq!(shared.redirect, None);
            assert_eq!(shared.robot_flags, None);
            assert_eq!(shared.original, None);
            assert_eq!(serde_json::to_value(shared.extra).unwrap(), unknown);
        }
    }
}

#[test]
fn partial_revisit_locations_and_integer_limits_survive_conversion() {
    let entry = Entry::try_from(cdxj_entry(json!({"orig.filename": "data.warc"}))).unwrap();
    let original = entry.original.unwrap();
    assert_eq!(original.filename.as_deref(), Some("data.warc"));
    assert_eq!(original.length, None);
    assert_eq!(original.offset, None);

    let entry = Entry::try_from(cdxj_entry(json!({
        "orig.length": u64::MAX,
        "orig.offset": "18446744073709551615"
    })))
    .unwrap();
    let original = entry.original.unwrap();
    assert_eq!(original.length, Some(u64::MAX));
    assert_eq!(original.offset, Some(u64::MAX));
    assert_eq!(original.filename, None);
}

#[test]
fn malformed_metadata_is_rejected_during_shared_conversion() {
    for name in ["redirect", "robotflags", "meta", "orig.filename"] {
        for value in [json!(17), json!(false), json!([]), json!({})] {
            assert!(matches!(
                Entry::try_from(cdxj_entry(json!({name: value}))),
                Err(field::Error::Invalid { field, .. }) if field == name
            ));
        }
    }
    for name in ["orig.length", "orig.offset"] {
        for value in [
            json!("invalid"),
            json!("18446744073709551616"),
            json!(1.5),
            json!(true),
        ] {
            assert!(matches!(
                Entry::try_from(cdxj_entry(json!({name: value}))),
                Err(field::Error::Invalid { field, .. }) if field == name
            ));
        }
    }
    for value in [json!(-1), json!("-1")] {
        assert!(matches!(
            Entry::try_from(cdxj_entry(json!({"orig.offset": value}))),
            Err(field::Error::Invalid {
                field: "orig.offset",
                ..
            })
        ));
    }
}
