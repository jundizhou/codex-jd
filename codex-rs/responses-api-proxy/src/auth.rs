use tiny_http::Header;

/// Checks the optional worker shared secret against the request's bearer token.
///
/// Authentication is disabled when `expected` is `None`, preserving the local
/// development behavior of older proxy invocations. When configured, exactly
/// one Authorization header with the `Bearer` scheme is required.
pub(crate) fn is_authorized(headers: &[Header], expected: Option<&str>) -> bool {
    let Some(expected) = expected else {
        return true;
    };

    let mut authorization = None;
    for header in headers {
        if !header.field.equiv("Authorization") {
            continue;
        }
        if authorization.is_some() {
            return false;
        }
        authorization = Some(header.value.as_str());
    }

    let Some(value) = authorization else {
        return false;
    };
    let Some((scheme, token)) = value.split_once(' ') else {
        return false;
    };
    scheme.eq_ignore_ascii_case("Bearer") && !token.is_empty() && token == expected
}

#[cfg(test)]
#[path = "auth_tests.rs"]
mod tests;
