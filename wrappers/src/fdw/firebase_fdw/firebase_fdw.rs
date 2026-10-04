use crate::stats;
use pgrx::{JsonB, PgBuiltInOids, PgOid, datetime::ToIsoString, pg_sys, prelude::*};
use regex::Regex;
use reqwest::{self, header};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware, RequestBuilder};
use reqwest_retry::{RetryTransientMiddleware, policies::ExponentialBackoff};
use serde_json::{Value as JsonValue, json};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use yup_oauth2::AccessToken;
use yup_oauth2::ServiceAccountAuthenticator;

use supabase_wrappers::prelude::*;

use super::{FirebaseFdwError, FirebaseFdwResult};

/// Default maximum response size in bytes (10 MB) to prevent DoS via large responses
const DEFAULT_MAX_RESPONSE_SIZE: usize = 10 * 1024 * 1024;

/// Maximum number of keys in a pushed down lookup, more keys fall back to a full scan
const MAX_LOOKUP_KEYS: usize = 100;

/// Maximum number of values in a Firestore `IN` filter
/// ref: https://firebase.google.com/docs/firestore/query-data/queries#limitations
const MAX_IN_VALUES: usize = 30;

/// Columns of a Firestore document table which are not mapped to document fields
const DOCUMENT_COLUMNS: [&str; 5] = ["name", "fields", "created_at", "updated_at", "attrs"];

fn get_oauth2_token(sa_key: &str, rt: &Runtime) -> FirebaseFdwResult<AccessToken> {
    let creds = yup_oauth2::parse_service_account_key(sa_key.as_bytes())?;
    let sa = rt.block_on(ServiceAccountAuthenticator::builder(creds).build())?;
    let scopes = &[
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/firebase.database",
        "https://www.googleapis.com/auth/firebase.messaging",
        "https://www.googleapis.com/auth/identitytoolkit",
        "https://www.googleapis.com/auth/userinfo.email",
    ];
    Ok(rt.block_on(sa.token(scopes))?)
}

fn body_to_rows(
    resp: &JsonValue,
    obj_key: &str,
    normal_cols: Vec<(&str, &str, &str)>,
    tgt_cols: &[Column],
    map_fields: bool,
) -> FirebaseFdwResult<Vec<Row>> {
    let mut result = Vec::new();

    let resp_obj = resp
        .as_object()
        .ok_or_else(|| FirebaseFdwError::InvalidResponse(resp.to_string()))?;

    // the key is left out when there are no objects, e.g. an empty collection
    let Some(objs) = resp_obj.get(obj_key) else {
        return Ok(result);
    };
    let objs = objs
        .as_array()
        .ok_or_else(|| FirebaseFdwError::InvalidResponse(resp.to_string()))?;

    for obj in objs {
        let mut row = Row::new();

        // extract normal columns
        for tgt_col in tgt_cols {
            if let Some((src_name, col_name, col_type)) =
                normal_cols.iter().find(|(_, c, _)| c == &tgt_col.name)
            {
                let v = obj
                    .as_object()
                    .and_then(|v| v.get(*src_name))
                    .ok_or(FirebaseFdwError::InvalidResponse(resp.to_string()))?;
                let cell = match *col_type {
                    "bool" => v.as_bool().map(Cell::Bool),
                    "i64" => v.as_i64().map(Cell::I64),
                    "string" => v.as_str().map(|a| Cell::String(a.to_owned())),
                    "timestamp" => Some(
                        v.as_str()
                            .and_then(|a| a.parse::<i64>().ok())
                            .map(|ms| to_timestamp(ms as f64 / 1000.0).to_utc())
                            .map(Cell::Timestamp)
                            .ok_or(FirebaseFdwError::InvalidTimestampFormat(v.to_string()))?,
                    ),
                    "timestamp_iso" => Some(
                        v.as_str()
                            .and_then(|a| Timestamp::from_str(a).ok())
                            .map(Cell::Timestamp)
                            .ok_or(FirebaseFdwError::InvalidTimestampFormat(v.to_string()))?,
                    ),
                    "json" => Some(Cell::Json(JsonB(v.clone()))),
                    _ => {
                        return Err(FirebaseFdwError::UnsupportedColumnType(format!(
                            "{col_name}({col_type})"
                        )));
                    }
                };
                row.push(col_name, cell);
            } else if map_fields && !DOCUMENT_COLUMNS.contains(&tgt_col.name.as_str()) {
                // other columns are mapped to the document fields with the same name
                let value = obj.get("fields").and_then(|v| v.get(&tgt_col.name));
                row.push(&tgt_col.name, field_to_cell(value, tgt_col)?);
            }
        }

        // put all properties into 'attrs' JSON column
        if tgt_cols.iter().any(|c| &c.name == "attrs") {
            let attrs = serde_json::from_str(&obj.to_string())?;
            row.push("attrs", Some(Cell::Json(JsonB(attrs))));
        }

        result.push(row);
    }

    Ok(result)
}

// convert response body text to rows
fn resp_to_rows(obj: &str, resp: &JsonValue, tgt_cols: &[Column]) -> FirebaseFdwResult<Vec<Row>> {
    match obj {
        "auth/users" => body_to_rows(
            resp,
            "users",
            vec![
                ("localId", "uid", "string"),
                ("email", "email", "string"),
                ("createdAt", "created_at", "timestamp"),
            ],
            tgt_cols,
            false,
        ),
        _ => {
            // match firestore documents
            if obj.starts_with("firestore/") {
                body_to_rows(
                    resp,
                    "documents",
                    vec![
                        ("name", "name", "string"),
                        ("fields", "fields", "json"),
                        ("createTime", "created_at", "timestamp_iso"),
                        ("updateTime", "updated_at", "timestamp_iso"),
                    ],
                    tgt_cols,
                    true,
                )
            } else {
                Err(FirebaseFdwError::ObjectNotImplemented(obj.to_string()))
            }
        }
    }
}

// get the values of an `=` or `IN (...)` qual on the key column `field`, so only
// the objects with those keys need to be fetched, Postgres still rechecks all quals
fn key_values(quals: &[Qual], field: &str) -> Option<Vec<String>> {
    quals
        .iter()
        .filter(|qual| qual.field == field && qual.operator == "=")
        .find_map(|qual| {
            let mut values = match &qual.value {
                Value::Cell(Cell::String(s)) if !qual.use_or => vec![s.clone()],
                Value::Array(cells) if qual.use_or => cells
                    .iter()
                    .map(|cell| match cell {
                        Cell::String(s) => Some(s.clone()),
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>()?,
                _ => return None,
            };
            values.sort();
            values.dedup();
            (values.len() <= MAX_LOOKUP_KEYS).then_some(values)
        })
}

// convert a Firestore document field value to a cell of the column type, a missing
// field or a null value is NULL
// ref: https://firebase.google.com/docs/firestore/reference/rest/v1/Value
fn field_to_cell(value: Option<&JsonValue>, col: &Column) -> FirebaseFdwResult<Option<Cell>> {
    let Some(value) = value.filter(|v| v.get("nullValue").is_none()) else {
        return Ok(None);
    };

    let string = |key: &str| value.get(key).and_then(|v| v.as_str());
    let integer = || string("integerValue").and_then(|v| v.parse::<i64>().ok());
    // NaN and infinities are encoded as strings
    let double = || {
        let double = value.get("doubleValue");
        double
            .and_then(|v| v.as_f64())
            .or_else(|| double.and_then(|v| v.as_str()?.parse().ok()))
            .or_else(|| integer().map(|v| v as f64))
    };

    let cell = match PgOid::from(col.type_oid) {
        PgOid::BuiltIn(PgBuiltInOids::BOOLOID) => value
            .get("booleanValue")
            .and_then(|v| v.as_bool())
            .map(Cell::Bool),
        PgOid::BuiltIn(PgBuiltInOids::INT2OID) => {
            integer().and_then(|v| v.try_into().ok()).map(Cell::I16)
        }
        PgOid::BuiltIn(PgBuiltInOids::INT4OID) => {
            integer().and_then(|v| v.try_into().ok()).map(Cell::I32)
        }
        PgOid::BuiltIn(PgBuiltInOids::INT8OID) => integer().map(Cell::I64),
        PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID) => double().map(|v| Cell::F32(v as f32)),
        PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID) => double().map(Cell::F64),
        PgOid::BuiltIn(PgBuiltInOids::TEXTOID) | PgOid::BuiltIn(PgBuiltInOids::VARCHAROID) => {
            string("stringValue").map(|v| Cell::String(v.to_owned()))
        }
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => string("timestampValue")
            .and_then(|v| Timestamp::from_str(v).ok())
            .map(Cell::Timestamp),
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID) => string("timestampValue")
            .and_then(|v| TimestampWithTimeZone::from_str(v).ok())
            .map(Cell::Timestamptz),
        PgOid::BuiltIn(PgBuiltInOids::JSONBOID) => Some(Cell::Json(JsonB(value.clone()))),
        _ => return Err(FirebaseFdwError::UnsupportedColumnType(col.name.clone())),
    };

    cell.map(Some)
        .ok_or_else(|| FirebaseFdwError::FieldTypeMismatch(col.name.clone(), value.to_string()))
}

// convert a qual value to a Firestore value. Postgres orders strings by collation
// and NaN above all floats, so those are only converted for equality filters.
fn cell_to_value(cell: &Cell, for_range: bool) -> Option<JsonValue> {
    // Firestore timestamps are in UTC, from year 1 to 9999
    let timestamp = |ts: Timestamp| {
        (ts.is_finite() && (1..=9999).contains(&ts.year()))
            .then(|| json!({ "timestampValue": format!("{}Z", ts.to_iso_string()) }))
    };

    match cell {
        Cell::I8(v) => Some(json!({ "integerValue": v.to_string() })),
        Cell::I16(v) => Some(json!({ "integerValue": v.to_string() })),
        Cell::I32(v) => Some(json!({ "integerValue": v.to_string() })),
        Cell::I64(v) => Some(json!({ "integerValue": v.to_string() })),
        Cell::Timestamp(v) => timestamp(*v),
        Cell::Timestamptz(v) if v.is_finite() => timestamp(v.to_utc()),
        _ if for_range => None,
        Cell::Bool(v) => Some(json!({ "booleanValue": v })),
        Cell::F32(v) if v.is_finite() => Some(json!({ "doubleValue": *v as f64 })),
        Cell::F64(v) if v.is_finite() => Some(json!({ "doubleValue": v })),
        Cell::String(v) => Some(json!({ "stringValue": v })),
        _ => None,
    }
}

// quote a document field name for a field path, unless it is a simple name
// ref: https://firebase.google.com/docs/firestore/reference/rest/v1/StructuredQuery#FieldReference
fn field_path(name: &str) -> String {
    let mut chars = name.chars();
    let simple = chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if simple {
        name.to_string()
    } else {
        format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
    }
}

// get the Firestore filters on the document fields from the quals, and the field of
// the range filters if any. Only the filters Firestore can serve from its automatic
// single-field indexes are pushed down: the equality filters on any fields, or else
// the range filters on one field, as combining them needs a composite index.
// ref: https://firebase.google.com/docs/firestore/query-data/index-overview
fn field_filters(quals: &[Qual]) -> (Vec<JsonValue>, Option<&str>) {
    let filter = |field: &str, op: &str, value: JsonValue| {
        json!({
            "fieldFilter": {
                "field": { "fieldPath": field_path(field) },
                "op": op,
                "value": value,
            }
        })
    };

    let mut equalities = Vec::new();
    let mut ranges = Vec::new();
    for qual in quals {
        let field = qual.field.as_str();
        if DOCUMENT_COLUMNS.contains(&field) {
            continue;
        }

        match (&qual.value, qual.operator.as_str(), qual.use_or) {
            (Value::Cell(cell), "=", false) => {
                if let Some(value) = cell_to_value(cell, false) {
                    equalities.push(filter(field, "EQUAL", value));
                }
            }
            // Firestore allows only one `IN` filter in a query
            (Value::Array(cells), "=", true)
                if (1..=MAX_IN_VALUES).contains(&cells.len())
                    && !equalities.iter().any(|f| f["fieldFilter"]["op"] == "IN") =>
            {
                let values = cells
                    .iter()
                    .map(|cell| cell_to_value(cell, false))
                    .collect::<Option<Vec<_>>>();
                if let Some(mut values) = values {
                    // duplicated values are rejected
                    values.sort_by_key(|v| v.to_string());
                    values.dedup();
                    let values = json!({ "arrayValue": { "values": values } });
                    equalities.push(filter(field, "IN", values));
                }
            }
            (Value::Cell(cell), op, false) => {
                let op = match op {
                    "<" => "LESS_THAN",
                    "<=" => "LESS_THAN_OR_EQUAL",
                    ">" => "GREATER_THAN",
                    ">=" => "GREATER_THAN_OR_EQUAL",
                    _ => continue,
                };
                if let Some(value) = cell_to_value(cell, true) {
                    ranges.push((field, filter(field, op, value)));
                }
            }
            _ => {}
        }
    }

    if !equalities.is_empty() {
        return (equalities, None);
    }
    let Some(&(range_field, _)) = ranges.first() else {
        return (Vec::new(), None);
    };
    let ranges = ranges
        .into_iter()
        .filter(|(field, _)| *field == range_field)
        .map(|(_, filter)| filter)
        .collect();
    (ranges, Some(range_field))
}

#[wrappers_fdw(
    version = "0.1.4",
    author = "Supabase",
    website = "https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/firebase_fdw",
    error_type = "FirebaseFdwError"
)]
pub(crate) struct FirebaseFdw {
    rt: Runtime,
    project_id: String,
    client: Option<ClientWithMiddleware>,
    scan_result: Vec<Row>,
    max_response_size: usize,
}

impl FirebaseFdw {
    const FDW_NAME: &'static str = "FirebaseFdw";

    const DEFAULT_AUTH_BASE_URL: &'static str =
        "https://identitytoolkit.googleapis.com/v1/projects";
    const DEFAULT_FIRESTORE_BASE_URL: &'static str =
        "https://firestore.googleapis.com/v1beta1/projects";

    // maximum allowed page size
    // https://firebase.google.com/docs/reference/admin/node/firebase-admin.auth.baseauth.md#baseauthlistusers
    const PAGE_SIZE: usize = 1000;

    // page size of document queries, small in tests so paging through the results
    // is tested too (the Firestore emulator doesn't page document lists)
    #[cfg(not(feature = "pg_test"))]
    const QUERY_PAGE_SIZE: usize = Self::PAGE_SIZE;
    #[cfg(feature = "pg_test")]
    const QUERY_PAGE_SIZE: usize = 2;

    // default maximum row count limit
    const DEFAULT_ROWS_LIMIT: usize = 10_000;

    fn build_url(
        &self,
        obj: &str,
        next_page: &Option<String>,
        options: &HashMap<String, String>,
    ) -> String {
        match obj {
            "auth/users" => {
                // ref: https://firebase.google.com/docs/reference/admin/node/firebase-admin.auth.baseauth.md#baseauthlistusers
                let base_url = options
                    .get("base_url")
                    .map(|t| t.to_owned())
                    .unwrap_or_else(|| Self::DEFAULT_AUTH_BASE_URL.to_owned());
                let mut ret = format!(
                    "{}/{}/accounts:batchGet?maxResults={}",
                    base_url,
                    self.project_id,
                    Self::PAGE_SIZE,
                );
                if let Some(next_page_token) = next_page {
                    ret.push_str(&format!("&nextPageToken={next_page_token}"));
                }
                ret
            }
            _ => {
                // match for firestore documents
                // ref: https://firebase.google.com/docs/firestore/reference/rest/v1beta1/projects.databases.documents/listDocuments
                let re = Regex::new(r"^firestore/(?P<collection>.+)").expect("regex is valid");
                if let Some(caps) = re.captures(obj) {
                    let base_url =
                        require_option_or("base_url", options, Self::DEFAULT_FIRESTORE_BASE_URL);
                    let collection = caps
                        .name("collection")
                        .expect("`collection` capture group always exists in a match")
                        .as_str();
                    let mut ret = format!(
                        "{}/{}/databases/(default)/documents/{}?pageSize={}",
                        base_url,
                        self.project_id,
                        collection,
                        Self::PAGE_SIZE,
                    );
                    if let Some(next_page_token) = next_page {
                        ret.push_str(&format!("&pageToken={next_page_token}"));
                    }
                    return ret;
                }

                "".to_string()
            }
        }
    }

    // fetch only the objects whose keys are in the quals instead of listing all
    // objects, returns None if the quals have no keys to look up
    fn lookup(
        &self,
        client: &ClientWithMiddleware,
        obj: &str,
        quals: &[Qual],
        options: &HashMap<String, String>,
    ) -> FirebaseFdwResult<Option<JsonValue>> {
        if obj == "auth/users" {
            let (key, values) = match (key_values(quals, "uid"), key_values(quals, "email")) {
                (Some(uids), _) => ("localId", uids),
                (None, Some(emails)) => ("email", emails),
                (None, None) => return Ok(None),
            };
            if values.is_empty() {
                return Ok(Some(json!({})));
            }

            // ref: https://cloud.google.com/identity-platform/docs/reference/rest/v1/accounts/lookup
            let base_url = require_option_or("base_url", options, Self::DEFAULT_AUTH_BASE_URL);
            let url = format!("{}/{}/accounts:lookup", base_url, self.project_id);
            let mut resp =
                self.fetch_json(Self::post_json(client, &url, json!({ key: values })))?;

            // the same user can be found by more than one key, e.g. emails
            // differing in case only
            if let Some(users) = resp.get_mut("users").and_then(|v| v.as_array_mut()) {
                let mut seen = HashSet::new();
                users.retain(|user| seen.insert(user.get("localId").cloned()));
            }

            return Ok(Some(resp));
        }

        if let Some(collection) = obj.strip_prefix("firestore/") {
            let Some(names) = key_values(quals, "name") else {
                return Ok(None);
            };

            // only the documents directly in this collection can match
            let prefix = format!(
                "projects/{}/databases/(default)/documents/{}/",
                self.project_id, collection
            );
            let names = names
                .into_iter()
                .filter(|name| {
                    name.strip_prefix(&prefix)
                        .is_some_and(|id| !id.is_empty() && !id.contains('/'))
                })
                .collect::<Vec<_>>();
            if names.is_empty() {
                return Ok(Some(json!({})));
            }

            // ref: https://firebase.google.com/docs/firestore/reference/rest/v1beta1/projects.databases.documents/batchGet
            let base_url = require_option_or("base_url", options, Self::DEFAULT_FIRESTORE_BASE_URL);
            let url = format!(
                "{}/{}/databases/(default)/documents:batchGet",
                base_url, self.project_id
            );
            let resp =
                self.fetch_json(Self::post_json(client, &url, json!({ "documents": names })))?;

            // keep the found documents, in the same format as a documents list
            let docs = resp
                .as_array()
                .ok_or_else(|| FirebaseFdwError::InvalidResponse(resp.to_string()))?
                .iter()
                .filter_map(|v| v.get("found").cloned())
                .collect::<Vec<_>>();

            return Ok(Some(json!({ "documents": docs })));
        }

        Ok(None)
    }

    // query the documents with the filters on their fields instead of listing all
    // documents, returns None if the quals have no filters to push down
    fn query_documents(
        &self,
        client: &ClientWithMiddleware,
        obj: &str,
        quals: &[Qual],
        columns: &[Column],
        row_cnt_limit: usize,
        options: &HashMap<String, String>,
    ) -> FirebaseFdwResult<Option<Vec<Row>>> {
        let Some(collection) = obj.strip_prefix("firestore/") else {
            return Ok(None);
        };
        let (filters, range_field) = field_filters(quals);
        if filters.is_empty() {
            return Ok(None);
        }

        // a nested collection is queried from its parent document, e.g. 'a/b/c'
        // is collection 'c' in document 'a/b'
        let (parent, collection_id) = match collection.rsplit_once('/') {
            Some((parent, collection_id)) => (format!("/{parent}"), collection_id),
            None => (String::new(), collection),
        };

        // ref: https://firebase.google.com/docs/firestore/reference/rest/v1beta1/projects.databases.documents/runQuery
        let base_url = require_option_or("base_url", options, Self::DEFAULT_FIRESTORE_BASE_URL);
        let url = format!(
            "{}/{}/databases/(default)/documents{}:runQuery",
            base_url, self.project_id, parent
        );

        // order by the range filter field, which Firestore requires to be first, and
        // the document name, so the next page can start after the last document
        let mut order_by = Vec::new();
        if let Some(field) = range_field {
            order_by.push(json!({ "field": { "fieldPath": field_path(field) } }));
        }
        order_by.push(json!({ "field": { "fieldPath": "__name__" } }));

        let mut query = json!({
            "from": [{ "collectionId": collection_id }],
            "where": { "compositeFilter": { "op": "AND", "filters": filters } },
            "orderBy": order_by,
            "limit": Self::QUERY_PAGE_SIZE,
        });

        let mut result = Vec::new();
        loop {
            let body = json!({ "structuredQuery": query });
            let resp = self.fetch_json(Self::post_json(client, &url, body))?;
            let docs = resp
                .as_array()
                .ok_or_else(|| FirebaseFdwError::InvalidResponse(resp.to_string()))?
                .iter()
                .filter_map(|v| v.get("document").cloned())
                .collect::<Vec<_>>();

            let page_len = docs.len();
            let cursor = docs.last().map(|doc| {
                let mut values = Vec::new();
                if let Some(field) = range_field {
                    values.push(doc["fields"][field].clone());
                }
                values.push(json!({ "referenceValue": doc["name"] }));
                json!({ "values": values, "before": false })
            });

            result.append(&mut resp_to_rows(
                obj,
                &json!({ "documents": docs }),
                columns,
            )?);

            // continue after the last document if the page is full
            match cursor {
                Some(cursor)
                    if page_len == Self::QUERY_PAGE_SIZE && result.len() < row_cnt_limit =>
                {
                    query["startAt"] = cursor;
                }
                _ => break,
            }
        }

        Ok(Some(result))
    }

    fn post_json(client: &ClientWithMiddleware, url: &str, body: JsonValue) -> RequestBuilder {
        client
            .post(url)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body.to_string())
    }

    // send a request and parse its JSON response
    fn fetch_json(&self, req: RequestBuilder) -> FirebaseFdwResult<JsonValue> {
        let body = self.rt.block_on(req.send()).and_then(|resp| {
            stats::inc_stats(
                Self::FDW_NAME,
                stats::Metric::BytesIn,
                resp.content_length().unwrap_or(0) as i64,
            );

            resp.error_for_status()
                .and_then(|resp| self.rt.block_on(resp.text()))
                .map_err(reqwest_middleware::Error::from)
        })?;

        // Security: Check response size to prevent DoS
        if body.len() > self.max_response_size {
            return Err(FirebaseFdwError::ResponseTooLarge(
                body.len(),
                self.max_response_size,
            ));
        }

        Ok(serde_json::from_str(&body)?)
    }
}

impl ForeignDataWrapper<FirebaseFdwError> for FirebaseFdw {
    fn new(server: ForeignServer) -> FirebaseFdwResult<Self> {
        // Security: Configure max response size (default 10 MB)
        let max_response_size = server
            .options
            .get("max_response_size")
            .map(|s| {
                s.parse::<usize>().map_err(|_| {
                    FirebaseFdwError::OptionsError(OptionsError::OptionParsingError {
                        option_name: "max_response_size".to_string(),
                        type_name: "usize",
                    })
                })
            })
            .transpose()?
            .unwrap_or(DEFAULT_MAX_RESPONSE_SIZE);

        let mut ret = Self {
            rt: create_async_runtime()?,
            project_id: require_option("project_id", &server.options)?.to_string(),
            client: None,
            scan_result: Vec::default(),
            max_response_size,
        };

        // get oauth2 access token if it is directly defined in options
        let token = if let Some(access_token) = server.options.get("access_token") {
            access_token.to_owned()
        } else {
            // otherwise, get it from the options or Vault
            let sa_key = match server.options.get("sa_key") {
                Some(sa_key) => sa_key.to_owned(),
                None => {
                    let sa_key_id = require_option("sa_key_id", &server.options)?;
                    match get_vault_secret(sa_key_id) {
                        Some(sa_key) => sa_key,
                        None => return Ok(ret),
                    }
                }
            };
            let access_token = get_oauth2_token(&sa_key, &ret.rt)?;
            access_token
                .token()
                .map(|t| t.to_owned())
                .ok_or(FirebaseFdwError::NoTokenFound(access_token))?
        };

        // create client
        let mut headers = header::HeaderMap::new();
        let value = format!("Bearer {token}");
        let mut auth_value = header::HeaderValue::from_str(&value)
            .map_err(|_| FirebaseFdwError::InvalidApiKeyHeader)?;
        auth_value.set_sensitive(true);
        headers.insert(header::AUTHORIZATION, auth_value);
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
        let client = ClientBuilder::new(client)
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();
        ret.client = Some(client);

        stats::inc_stats(Self::FDW_NAME, stats::Metric::CreateTimes, 1);

        Ok(ret)
    }

    fn begin_scan(
        &mut self,
        quals: &[Qual],
        columns: &[Column],
        _sorts: &[Sort],
        _limit: &Option<Limit>,
        options: &HashMap<String, String>,
    ) -> FirebaseFdwResult<()> {
        let obj = require_option("object", options)?;
        let row_cnt_limit = options
            .get("limit")
            .map(|n| n.parse::<usize>())
            .transpose()?
            .unwrap_or(Self::DEFAULT_ROWS_LIMIT);

        self.scan_result = Vec::new();

        if let Some(client) = &self.client {
            let mut result = Vec::new();

            if let Some(json) = self.lookup(client, obj, quals, options)? {
                // the quals are pushed down, so only the matching objects are fetched
                result = resp_to_rows(obj, &json, columns)?;
            } else if let Some(rows) =
                self.query_documents(client, obj, quals, columns, row_cnt_limit, options)?
            {
                result = rows;
            } else {
                let mut next_page: Option<String> = None;

                loop {
                    let url = self.build_url(obj, &next_page, options);
                    let json = self.fetch_json(client.get(&url))?;
                    let mut rows = resp_to_rows(obj, &json, columns)?;
                    result.append(&mut rows);
                    if result.len() >= row_cnt_limit {
                        break;
                    }

                    // get next page token, stop fetching if no more pages
                    next_page = json
                        .get("nextPageToken")
                        .and_then(|v| v.as_str())
                        .map(|v| v.to_owned());
                    if next_page.is_none() {
                        break;
                    }
                }
            }

            stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsIn, result.len() as i64);
            stats::inc_stats(Self::FDW_NAME, stats::Metric::RowsOut, result.len() as i64);

            self.scan_result = result;
        }

        Ok(())
    }

    fn iter_scan(&mut self, row: &mut Row) -> FirebaseFdwResult<Option<()>> {
        if self.scan_result.is_empty() {
            Ok(None)
        } else {
            Ok(self
                .scan_result
                .drain(0..1)
                .next_back()
                .map(|src_row| row.replace_with(src_row)))
        }
    }

    fn end_scan(&mut self) -> FirebaseFdwResult<()> {
        Ok(())
    }

    fn validator(
        options: Vec<Option<String>>,
        catalog: Option<pg_sys::Oid>,
    ) -> FirebaseFdwResult<()> {
        if let Some(oid) = catalog
            && oid == FOREIGN_TABLE_RELATION_ID
        {
            check_options_contain(&options, "object")?;
        }

        Ok(())
    }
}
