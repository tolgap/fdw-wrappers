use crate::stats;
use pgrx::{JsonB, PgBuiltInOids, PgOid, datetime::ToIsoString, pg_sys, prelude::*};
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

/// Maximum number of values in a Firestore `IN` and `NOT_IN` filter
/// ref: https://firebase.google.com/docs/firestore/query-data/queries#limitations
const MAX_IN_VALUES: usize = 30;
const MAX_NOT_IN_VALUES: usize = 10;

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
                row.push(&tgt_col.name, value_to_cell(value, tgt_col)?);
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

/// Convert one Firestore document field value into a `Cell` matching the declared
/// column type. Returns Ok(None) when the field is missing or `null`.
/// ref: https://firebase.google.com/docs/firestore/reference/rest/v1/Value
fn value_to_cell(value: Option<&JsonValue>, tgt_col: &Column) -> FirebaseFdwResult<Option<Cell>> {
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

    let cell = match PgOid::from(tgt_col.type_oid) {
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
        _ => {
            return Err(FirebaseFdwError::UnsupportedColumnType(
                tgt_col.name.clone(),
            ));
        }
    };

    cell.map(Some)
        .ok_or_else(|| FirebaseFdwError::FieldTypeMismatch(tgt_col.name.clone(), value.to_string()))
}

/// Convert a `Cell` to a Firestore value. Returns `None` for the cells which can't
/// be compared in Firestore like in Postgres.
fn cell_to_value(cell: &Cell) -> Option<JsonValue> {
    // Firestore timestamps are in UTC, from year 1 to 9999
    let timestamp = |ts: Timestamp| {
        (ts.is_finite() && (1..=9999).contains(&ts.year()))
            .then(|| json!({ "timestampValue": format!("{}Z", ts.to_iso_string()) }))
    };

    match cell {
        Cell::Bool(v) => Some(json!({ "booleanValue": v })),
        Cell::I8(v) => Some(json!({ "integerValue": v.to_string() })),
        Cell::I16(v) => Some(json!({ "integerValue": v.to_string() })),
        Cell::I32(v) => Some(json!({ "integerValue": v.to_string() })),
        Cell::I64(v) => Some(json!({ "integerValue": v.to_string() })),
        // NaN can't be compared in Firestore
        Cell::F32(v) if v.is_finite() => Some(json!({ "doubleValue": *v as f64 })),
        Cell::F64(v) if v.is_finite() => Some(json!({ "doubleValue": v })),
        Cell::String(v) => Some(json!({ "stringValue": v })),
        Cell::Timestamp(v) => timestamp(*v),
        Cell::Timestamptz(v) if v.is_finite() => timestamp(v.to_utc()),
        _ => None,
    }
}

/// Quote a document field name for a field path, unless it is a simple name
/// ref: https://firebase.google.com/docs/firestore/reference/rest/v1/StructuredQuery#FieldReference
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

/// Translate a single qual to a Firestore filter and its operator. Returns `None`
/// when the qual cannot be pushed down (caller drops it; Postgres re-checks).
/// `collection` is the resource name of the queried collection.
/// ref: https://firebase.google.com/docs/firestore/reference/rest/v1/StructuredQuery#Filter
fn qual_to_filter(qual: &Qual, collection: &str) -> Option<(&'static str, JsonValue)> {
    let field = qual.field.as_str();

    // The document name is filtered as `__name__`, only by `=` and `IN` with the
    // names of documents in this collection. The other metadata can't be filtered.
    let is_name = field == "name";
    if !is_name && DOCUMENT_COLUMNS.contains(&field) {
        return None;
    }
    let path = if is_name {
        "__name__".to_string()
    } else {
        field_path(field)
    };
    let to_value = |cell: &Cell| match cell {
        Cell::String(name) if is_name => name
            .strip_prefix(collection)
            .and_then(|id| id.strip_prefix('/'))
            .is_some_and(|id| !id.is_empty() && !id.contains('/'))
            .then(|| json!({ "referenceValue": name })),
        _ if is_name => None,
        _ => cell_to_value(cell),
    };
    let to_array = |cells: &[Cell]| {
        let mut values = cells.iter().map(to_value).collect::<Option<Vec<_>>>()?;
        values.sort_by_key(|v| v.to_string());
        values.dedup();
        Some(json!({ "arrayValue": { "values": values } }))
    };

    let (op, value) = match (&qual.value, qual.operator.as_str(), qual.use_or) {
        (Value::Cell(cell), "=", false) => ("EQUAL", to_value(cell)?),
        (Value::Cell(cell), "<>" | "!=", false) if !is_name => ("NOT_EQUAL", to_value(cell)?),
        // Postgres orders text by collation and NaN above all floats, unlike
        // Firestore, so only integers and timestamps are compared by order
        (
            Value::Cell(
                cell @ (Cell::I8(_)
                | Cell::I16(_)
                | Cell::I32(_)
                | Cell::I64(_)
                | Cell::Timestamp(_)
                | Cell::Timestamptz(_)),
            ),
            op,
            false,
        ) if !is_name => {
            let op = match op {
                "<" => "LESS_THAN",
                "<=" => "LESS_THAN_OR_EQUAL",
                ">" => "GREATER_THAN",
                ">=" => "GREATER_THAN_OR_EQUAL",
                _ => return None,
            };
            (op, to_value(cell)?)
        }
        // `IS NOT NULL` excludes missing fields like Postgres does, but `IS NULL`
        // doesn't match them, so it isn't pushed down
        (Value::Cell(Cell::String(s)), "is not", false) if s == "null" && !is_name => {
            let filter =
                json!({ "unaryFilter": { "field": { "fieldPath": path }, "op": "IS_NOT_NULL" } });
            return Some(("IS_NOT_NULL", filter));
        }
        // `IN (...)` / `= ANY(ARRAY[...])`
        (Value::Array(cells), "=", true) if (1..=MAX_IN_VALUES).contains(&cells.len()) => {
            ("IN", to_array(cells)?)
        }
        // `NOT IN (...)` / `<> ALL(ARRAY[...])`
        (Value::Array(cells), "<>" | "!=", false)
            if !is_name && (1..=MAX_NOT_IN_VALUES).contains(&cells.len()) =>
        {
            ("NOT_IN", to_array(cells)?)
        }
        _ => return None,
    };

    let filter =
        json!({ "fieldFilter": { "field": { "fieldPath": path }, "op": op, "value": value } });
    Some((op, filter))
}

/// Translate the quals to a Firestore filter, AND'ing the filters of all quals which
/// can be pushed down. A query allows inequality filters on one field, one of
/// `NOT_EQUAL`/`NOT_IN`/`IS_NOT_NULL` and one of `IN`/`NOT_IN`, so the quals which
/// don't fit are left out too (Postgres re-checks them). Returns the filter and the
/// field of its inequality filters, if any.
/// ref: https://firebase.google.com/docs/firestore/query-data/queries#limitations
fn quals_to_filter<'a>(
    quals: &'a [Qual],
    collection: &str,
) -> (Option<JsonValue>, Option<&'a str>) {
    let mut filters = Vec::new();
    let mut inequality_field = None;
    let mut has_negation = false;
    let mut has_disjunction = false;

    for qual in quals {
        let Some((op, filter)) = qual_to_filter(qual, collection) else {
            continue;
        };
        let inequality = !matches!(op, "EQUAL" | "IN");
        let negation = matches!(op, "NOT_EQUAL" | "NOT_IN" | "IS_NOT_NULL");
        let disjunction = matches!(op, "IN" | "NOT_IN");
        if (inequality && inequality_field.is_some_and(|f| f != qual.field))
            || (negation && has_negation)
            || (disjunction && has_disjunction)
        {
            continue;
        }

        if inequality {
            inequality_field = Some(qual.field.as_str());
        }
        has_negation |= negation;
        has_disjunction |= disjunction;
        filters.push(filter);
    }

    let filter = match filters.len() {
        0 => None,
        1 => filters.pop(),
        _ => Some(json!({ "compositeFilter": { "op": "AND", "filters": filters } })),
    };
    (filter, inequality_field)
}

/// Build a Firestore projection from the columns mapped to document fields. Returns
/// `None` when an `attrs` or `fields` column is present — we need the full document
/// in that case.
fn columns_to_projection(columns: &[Column]) -> Option<JsonValue> {
    if columns
        .iter()
        .any(|c| c.name == "attrs" || c.name == "fields")
    {
        return None;
    }
    let mut fields = columns
        .iter()
        .filter(|c| !DOCUMENT_COLUMNS.contains(&c.name.as_str()))
        .map(|c| json!({ "fieldPath": field_path(&c.name) }))
        .collect::<Vec<_>>();
    // only the document names and metadata are needed
    if fields.is_empty() {
        fields.push(json!({ "fieldPath": "__name__" }));
    }
    Some(json!({ "fields": fields }))
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

    // default maximum row count limit
    const DEFAULT_ROWS_LIMIT: usize = 10_000;

    fn build_users_url(
        &self,
        next_page: &Option<String>,
        options: &HashMap<String, String>,
    ) -> String {
        // ref: https://firebase.google.com/docs/reference/admin/node/firebase-admin.auth.baseauth.md#baseauthlistusers
        let base_url = require_option_or("base_url", options, Self::DEFAULT_AUTH_BASE_URL);
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

    // fetch only the users whose uid or email are in the quals instead of listing all
    // users, returns None if the quals have no keys to look up
    fn lookup_users(
        &self,
        client: &ClientWithMiddleware,
        quals: &[Qual],
        options: &HashMap<String, String>,
    ) -> FirebaseFdwResult<Option<JsonValue>> {
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
        let mut resp = self.fetch_json(Self::post_json(client, &url, json!({ key: values })))?;

        // the same user can be found by more than one key, e.g. emails
        // differing in case only
        if let Some(users) = resp.get_mut("users").and_then(|v| v.as_array_mut()) {
            let mut seen = HashSet::new();
            users.retain(|user| seen.insert(user.get("localId").cloned()));
        }

        Ok(Some(resp))
    }

    // run a query on the Firestore collection, with the quals pushed down as its
    // filter and only the fields of the columns selected. The results are paged by
    // the inequality filter field and the document name.
    fn query_documents(
        &self,
        client: &ClientWithMiddleware,
        obj: &str,
        quals: &[Qual],
        columns: &[Column],
        row_cnt_limit: usize,
        options: &HashMap<String, String>,
    ) -> FirebaseFdwResult<Vec<Row>> {
        let collection = obj.strip_prefix("firestore/").unwrap_or(obj);

        // a nested collection is queried in its parent document, e.g. 'a/b/c' is
        // collection 'c' in document 'a/b'
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

        let collection_name = format!(
            "projects/{}/databases/(default)/documents/{}",
            self.project_id, collection
        );
        let (filter, inequality_field) = quals_to_filter(quals, &collection_name);

        // Firestore requires the inequality filter field to be ordered by first, and
        // the document name orders the rest, so a page can start after the last one
        let mut order_by = Vec::new();
        if let Some(field) = inequality_field {
            order_by.push(json!({ "field": { "fieldPath": field_path(field) } }));
        }
        order_by.push(json!({ "field": { "fieldPath": "__name__" } }));

        let mut query = json!({
            "from": [{ "collectionId": collection_id }],
            "orderBy": order_by,
            "limit": Self::PAGE_SIZE,
        });
        if let Some(filter) = filter {
            query["where"] = filter;
        }
        if let Some(projection) = columns_to_projection(columns) {
            query["select"] = projection;
        }

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
                if let Some(field) = inequality_field {
                    values.push(doc["fields"][field].clone());
                }
                values.push(json!({ "referenceValue": doc["name"] }));
                json!({ "values": values, "before": false })
            });

            let mut rows = resp_to_rows(obj, &json!({ "documents": docs }), columns)?;
            result.append(&mut rows);

            // continue after the last document if the page is full
            match cursor {
                Some(cursor) if page_len == Self::PAGE_SIZE && result.len() < row_cnt_limit => {
                    query["startAt"] = cursor;
                }
                _ => break,
            }
        }

        Ok(result)
    }

    fn post_json(client: &ClientWithMiddleware, url: &str, body: JsonValue) -> RequestBuilder {
        client
            .post(url)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body.to_string())
    }

    // send a request and parse its JSON response, an error response is returned with
    // its message, e.g. the composite index a Firestore query needs
    fn fetch_json(&self, req: RequestBuilder) -> FirebaseFdwResult<JsonValue> {
        let resp = self.rt.block_on(req.send())?;
        stats::inc_stats(
            Self::FDW_NAME,
            stats::Metric::BytesIn,
            resp.content_length().unwrap_or(0) as i64,
        );
        let status = resp.status();
        let body = self.rt.block_on(resp.text())?;

        // Security: Check response size to prevent DoS
        if body.len() > self.max_response_size {
            return Err(FirebaseFdwError::ResponseTooLarge(
                body.len(),
                self.max_response_size,
            ));
        }

        if !status.is_success() {
            // ref: https://cloud.google.com/apis/design/errors#http_mapping
            let message = serde_json::from_str::<JsonValue>(&body)
                .ok()
                .and_then(|v| v["error"]["message"].as_str().map(|s| s.to_owned()))
                .unwrap_or(body);
            return Err(FirebaseFdwError::ApiError(status, message));
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

            if obj.starts_with("firestore/") {
                result =
                    self.query_documents(client, obj, quals, columns, row_cnt_limit, options)?;
            } else if obj != "auth/users" {
                return Err(FirebaseFdwError::ObjectNotImplemented(obj.to_string()));
            } else if let Some(json) = self.lookup_users(client, quals, options)? {
                // the quals are pushed down, so only the matching users are fetched
                result = resp_to_rows(obj, &json, columns)?;
            } else {
                let mut next_page: Option<String> = None;

                loop {
                    let url = self.build_users_url(&next_page, options);
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
