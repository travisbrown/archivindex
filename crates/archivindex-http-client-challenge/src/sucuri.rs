//! Recognition of Sucuri `CloudProxy`'s JavaScript cookie challenge.
//!
//! The challenge page carries a base64-encoded script that sets one cookie and reloads. The decoder
//! below reads that script's cookie assignment under a grammar narrow enough that nothing else can
//! be expressed in it: literals, character codes, and one variable.

use std::time::Instant;

use archivindex_http_client::Exchange;
use data_encoding::BASE64;
use http::StatusCode;
use http::header::HeaderValue;
use url::Url;

use super::script::parse_string;
use super::{Challenge, CookieValue};

const MAX_RESPONSE_BYTES: usize = 256 * 1024;
const MAX_SCRIPT_BYTES: usize = 64 * 1024;
/// Includes cookie attributes and intermediate expression values.
const MAX_EXPRESSION_BYTES: usize = 4096;

/// Decode the cookie from Sucuri's JavaScript reload challenge without evaluating its script.
pub fn recognize(captured: &Exchange, _: &Url, deadline: Option<Instant>) -> Option<Challenge> {
    if captured.status != StatusCode::TEMPORARY_REDIRECT.as_u16()
        || captured.response.len() > MAX_RESPONSE_BYTES
        || captured
            .response_metadata()?
            .header("x-sucuri-id")
            .is_none()
    {
        return None;
    }

    parse_cookie(&captured.entity_body().ok()?, deadline).map(Challenge::Cookie)
}

fn parse_cookie(body: &[u8], deadline: Option<Instant>) -> Option<CookieValue> {
    if body.len() > MAX_RESPONSE_BYTES || deadline.is_some_and(|end| Instant::now() >= end) {
        return None;
    }
    let html = std::str::from_utf8(body).ok()?;
    let encoded = html.split_once("sucuri_cloudproxy_js=")?.1.trim_start();
    let (encoded, rest) = parse_string(encoded)?;
    let encoded = if encoded.is_empty() {
        parse_string(rest.trim_start().strip_prefix(",S=")?.trim_start())?.0
    } else {
        encoded
    };
    if encoded.len() > BASE64.encode_len(MAX_SCRIPT_BYTES) {
        return None;
    }
    let decoded = BASE64.decode(encoded.as_bytes()).ok()?;
    if decoded.len() > MAX_SCRIPT_BYTES {
        return None;
    }
    let script = std::str::from_utf8(&decoded).ok()?.trim_start();

    let (variable_name, value_expression) = script.split_once('=')?;
    if variable_name.is_empty()
        || variable_name.len() > 16
        || !variable_name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$'))
        || variable_name
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_digit)
    {
        return None;
    }
    let (challenge_value, _) = evaluate_js_statement(value_expression, None, deadline)?;
    let cookie_expression = script.split_once("document.cookie=")?.1;
    let (cookie, _) = evaluate_js_statement(
        cookie_expression,
        Some((variable_name, &challenge_value)),
        deadline,
    )?;
    let mut attributes = cookie.split(';');
    let pair = attributes.next()?.trim();
    let (name, value) = pair.split_once('=')?;
    if !name.starts_with("sucuri_cloudproxy_uuid_")
        || name.len() == "sucuri_cloudproxy_uuid_".len()
        || value.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        || deadline.is_some_and(|end| Instant::now() >= end)
    {
        return None;
    }

    Some(CookieValue {
        value: HeaderValue::from_str(pair).ok()?,
        secure: attributes.any(|attribute| attribute.trim().eq_ignore_ascii_case("secure")),
    })
}

/// Evaluate one semicolon-terminated concatenation of literals, one variable, and character codes.
fn evaluate_js_statement<'a>(
    mut input: &'a str,
    variable: Option<(&str, &str)>,
    deadline: Option<Instant>,
) -> Option<(String, &'a str)> {
    let mut output = String::new();

    loop {
        input = input.trim_start();
        if input
            .as_bytes()
            .first()
            .is_some_and(|byte| matches!(byte, b'\'' | b'"'))
        {
            let (value, rest) = parse_string(input)?;
            append(&mut output, &value, deadline)?;
            input = rest;
        } else if let Some(rest) = input
            .strip_prefix("String.fromCharCode(")
            .or_else(|| input.strip_prefix("w("))
        {
            let (arguments, rest) = rest.split_once(')')?;
            for argument in arguments.split(',') {
                let character = char::from_u32(argument.trim().parse().ok()?)?;
                append(&mut output, character.encode_utf8(&mut [0; 4]), deadline)?;
            }
            input = rest;
        } else {
            let (name, value) = variable?;
            let rest = input.strip_prefix(name)?;
            if rest
                .as_bytes()
                .first()
                .is_some_and(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$'))
            {
                return None;
            }
            append(&mut output, value, deadline)?;
            input = rest;
        }

        input = input.trim_start();
        if let Some(rest) = input.strip_prefix('+') {
            input = rest;
        } else {
            let rest = input.strip_prefix(';')?;
            return Some((output, rest));
        }
    }
}

/// Check expansion before allocating, including for repeated variable references.
fn append(output: &mut String, value: &str, deadline: Option<Instant>) -> Option<()> {
    if value.len() > MAX_EXPRESSION_BYTES - output.len()
        || deadline.is_some_and(|end| Instant::now() >= end)
    {
        None
    } else {
        output.push_str(value);
        Some(())
    }
}

#[cfg(test)]
mod tests {
    use data_encoding::BASE64;

    use super::{
        MAX_EXPRESSION_BYTES, MAX_RESPONSE_BYTES, MAX_SCRIPT_BYTES, evaluate_js_statement,
        parse_cookie,
    };

    #[test]
    fn cookie_decoder_handles_obfuscated_concatenation_without_evaluation() {
        let script = "r='b' + String.fromCharCode(51) + 'f';\
            document.cookie='s'+'u'+'c'+'u'+'r'+'i'+'_'+'c'+'l'+'o'+'u'+'d'+'p'+'r'+'o'+'x'+\
            'y'+'_'+'u'+'u'+'i'+'d'+'_'+'t'+'e'+'s'+'t'+'=' + r + \
            ';path=/;max-age=86400;SameSite=Lax;Secure'; location.reload();";
        let encoded = BASE64.encode(script.as_bytes());
        let html = format!("<script>sucuri_cloudproxy_js='',S='{encoded}';</script>");

        let cookie = parse_cookie(html.as_bytes(), None).expect("a recognized Sucuri challenge");

        assert_eq!(cookie.value, "sucuri_cloudproxy_uuid_test=b3f");
        assert!(cookie.secure);
    }

    #[test]
    fn cookie_decoder_rejects_arbitrary_script() {
        let encoded = BASE64.encode(b"fetch(\"https://example.com/\");");
        let html = format!("<script>sucuri_cloudproxy_js='{encoded}';</script>");

        assert!(parse_cookie(html.as_bytes(), None).is_none());
    }

    #[test]
    fn expression_limits_cover_literals_variables_and_character_codes() {
        let value = "a".repeat(MAX_EXPRESSION_BYTES);
        assert_eq!(
            evaluate_js_statement("r;", Some(("r", &value)), None)
                .unwrap()
                .0,
            value
        );
        assert!(evaluate_js_statement("r+r;", Some(("r", &value)), None).is_none());
        assert!(evaluate_js_statement(&format!("'{value}a';"), None, None).is_none());
        for function in ["String.fromCharCode", "w"] {
            let input = format!("'{value}'+{function}(97);");
            assert!(evaluate_js_statement(&input, None, None).is_none());
        }
    }

    #[test]
    fn oversized_responses_and_decoded_scripts_are_rejected() {
        let script = "r='a';document.cookie='sucuri_cloudproxy_uuid_test='+r;";
        for limit in [MAX_SCRIPT_BYTES, MAX_SCRIPT_BYTES + 1] {
            let script = format!("{script}{}", " ".repeat(limit - script.len()));
            let encoded = BASE64.encode(script.as_bytes());
            let html = format!("sucuri_cloudproxy_js='{encoded}';");
            assert_eq!(
                parse_cookie(html.as_bytes(), None).is_some(),
                limit == MAX_SCRIPT_BYTES
            );
        }
        let mut html = format!(
            "sucuri_cloudproxy_js='{}';",
            BASE64.encode(script.as_bytes())
        );
        html.extend(std::iter::repeat_n(' ', MAX_RESPONSE_BYTES));
        assert!(parse_cookie(html.as_bytes(), None).is_none());
    }

    #[test]
    fn expired_deadlines_stop_expression_expansion() {
        let deadline = Some(std::time::Instant::now());
        for input in ["'value';", "String.fromCharCode(97);", "w(97);", "r+r;"] {
            assert!(evaluate_js_statement(input, Some(("r", "value")), deadline).is_none());
        }
    }
}
