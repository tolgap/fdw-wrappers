#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::JsonB;
    use pgrx::prelude::*;
    use serde_json::{Value, json};
    use supabase_wrappers::prelude::{Runtime, create_async_runtime};

    const FIRESTORE_URL: &str =
        "http://localhost:8080/v1/projects/supa/databases/(default)/documents";
    const DOCUMENTS: &str = "projects/supa/databases/(default)/documents";

    #[pg_test]
    fn firebase_smoketest() {
        Spi::connect_mut(|c| {
            c.update(
                r#"CREATE FOREIGN DATA WRAPPER firebase_wrapper
                         HANDLER firebase_fdw_handler VALIDATOR firebase_fdw_validator"#,
                None,
                &[],
            )
            .unwrap();
            c.update(
                r#"CREATE SERVER my_firebase_server
                         FOREIGN DATA WRAPPER firebase_wrapper
                         OPTIONS (
                          project_id 'supa',
                          access_token 'owner'
                         )"#,
                None,
                &[],
            )
            .unwrap();

            /*
             The tables below come from the code in docker-compose.yml that looks like this:

             ```
             volumes:
                   - ../dockerfiles/firebase/baseline-data:/baseline-data
             ```
            */

            c.update(
                r#"
                  CREATE FOREIGN TABLE firebase_users (
                    uid text,
                    email text,
                    created_at timestamp,
                    attrs jsonb
                  )
                 SERVER my_firebase_server
                 OPTIONS (
                   object 'auth/users',
                   base_url 'http://localhost:9099/identitytoolkit.googleapis.com/v1/projects'
                )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT email FROM firebase_users order by email", None, &[])
                .unwrap()
                .filter_map(|r| r.get_by_name::<&str, _>("email").unwrap())
                .collect::<Vec<_>>();

            assert_eq!(results, vec!["bar@example.com", "foo@example.com"]);

            c.update(
                r#"
                CREATE FOREIGN TABLE firebase_docs (
                  name text,
                  created_at timestamp,
                  updated_at timestamp,
                  attrs jsonb
                )
                SERVER my_firebase_server
                OPTIONS (
                  object 'firestore/my-collection',  -- format: 'firestore/[collection_id]'
                  base_url 'http://localhost:8080/v1/projects'
                )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT name,attrs FROM firebase_docs", None, &[])
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("name").unwrap().zip(
                        r.get_by_name::<JsonB, _>("attrs")
                            .unwrap()
                            .map(|j| j.0.get("fields").unwrap().clone()),
                    )
                })
                .collect::<Vec<_>>();

            assert_eq!(
                results,
                vec![(
                    "projects/supa/databases/(default)/documents/my-collection/bSMScXpZHMJe9ilE9Yqs",
                    serde_json::json!({
                        "id": { "integerValue": "1" },
                        "name": { "stringValue": "hello" }
                    })
                )]
            );

            c.update(
                r#"
                CREATE FOREIGN TABLE firebase_docs_nested (
                  name text,
                  created_at timestamp,
                  updated_at timestamp,
                  attrs jsonb
                )
                SERVER my_firebase_server
                OPTIONS (
                  object 'firestore/my-collection/bSMScXpZHMJe9ilE9Yqs/my-collection2',
                  base_url 'http://localhost:8080/v1/projects'
                )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT name,attrs FROM firebase_docs_nested", None, &[])
                .unwrap()
                .filter_map(|r| {
                    r.get_by_name::<&str, _>("name").unwrap().zip(
                        r.get_by_name::<JsonB, _>("attrs")
                            .unwrap()
                            .map(|j| j.0.get("fields").unwrap().clone()),
                    )
                })
                .collect::<Vec<_>>();

            assert_eq!(
                results,
                vec![(
                    "projects/supa/databases/(default)/documents/my-collection/bSMScXpZHMJe9ilE9Yqs/my-collection2/fkSWL4hNJ3lRc1ZIorPm",
                    serde_json::json!({ "foo": { "stringValue": "bar" } })
                )]
            );
        });
    }

    async fn post_firestore(client: &reqwest::Client, url: &str, body: Value) -> Value {
        client
            .post(url)
            .bearer_auth("owner")
            .json(&body)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    /// Set up a Firestore emulator collection with the documents, deleting the
    /// existing ones first for idempotency. Each test passes a unique collection
    /// to avoid parallel-run collisions.
    fn setup_firestore_collection(rt: &Runtime, collection: &str, docs: Vec<(String, Value)>) {
        rt.block_on(async {
            let client = reqwest::Client::new();

            // a nested collection is queried in its parent document
            let (parent, collection_id) = match collection.rsplit_once('/') {
                Some((parent, collection_id)) => (format!("/{parent}"), collection_id),
                None => (String::new(), collection),
            };
            let existing = post_firestore(
                &client,
                &format!("{FIRESTORE_URL}{parent}:runQuery"),
                json!({ "structuredQuery": {
                    "from": [{ "collectionId": collection_id }],
                    "select": { "fields": [{ "fieldPath": "__name__" }] },
                } }),
            )
            .await;

            let deletes = existing
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|r| r["document"]["name"].as_str())
                .map(|name| json!({ "delete": name }))
                .collect::<Vec<_>>();
            let updates = docs
                .into_iter()
                .map(|(id, fields)| {
                    let name = format!("{DOCUMENTS}/{collection}/{id}");
                    json!({ "update": { "name": name, "fields": fields } })
                })
                .collect::<Vec<_>>();

            // a commit has at most 500 writes
            for writes in deletes.chunks(500).chain(updates.chunks(500)) {
                let url = format!("{FIRESTORE_URL}:commit");
                post_firestore(&client, &url, json!({ "writes": writes })).await;
            }
        });
    }

    /// Set up a people collection, where Dave has no `age` and Carol's `city` is null.
    fn setup_firestore_people(rt: &Runtime, collection: &str) {
        let person = |id: &str, fields: Value| (id.to_string(), fields);
        setup_firestore_collection(
            rt,
            collection,
            vec![
                person(
                    "alice",
                    json!({
                        "displayName": { "stringValue": "Alice" },
                        "age": { "integerValue": "30" },
                        "score": { "doubleValue": 9.5 },
                        "active": { "booleanValue": true },
                        "joined": { "timestampValue": "2020-01-15T10:00:00Z" },
                        "city": { "stringValue": "Amsterdam" },
                    }),
                ),
                person(
                    "bob",
                    json!({
                        "displayName": { "stringValue": "Bob" },
                        "age": { "integerValue": "25" },
                        "score": { "integerValue": "7" },
                        "active": { "booleanValue": false },
                        "joined": { "timestampValue": "2021-06-01T00:00:00Z" },
                        "city": { "stringValue": "Berlin" },
                    }),
                ),
                person(
                    "carol",
                    json!({
                        "displayName": { "stringValue": "Carol" },
                        "age": { "integerValue": "35" },
                        "score": { "doubleValue": 8.25 },
                        "active": { "booleanValue": true },
                        "joined": { "timestampValue": "2019-03-10T08:30:00Z" },
                        "city": { "nullValue": null },
                    }),
                ),
                person(
                    "dave",
                    json!({
                        "displayName": { "stringValue": "Dave" },
                        "score": { "doubleValue": 6.5 },
                        "active": { "booleanValue": true },
                        "joined": { "timestampValue": "2022-11-20T12:00:00Z" },
                        "city": { "stringValue": "Amsterdam" },
                    }),
                ),
                person(
                    "erin",
                    json!({
                        "displayName": { "stringValue": "erin" },
                        "age": { "integerValue": "41" },
                        "score": { "doubleValue": 7.75 },
                        "active": { "booleanValue": false },
                        "joined": { "timestampValue": "2023-08-05T16:45:00Z" },
                        "city": { "stringValue": "Berlin" },
                    }),
                ),
            ],
        );
    }

    fn create_server(c: &mut pgrx::spi::SpiClient) {
        c.update(
            r#"CREATE FOREIGN DATA WRAPPER firebase_wrapper
                HANDLER firebase_fdw_handler VALIDATOR firebase_fdw_validator"#,
            None,
            &[],
        )
        .unwrap();
        c.update(
            r#"CREATE SERVER firebase_server FOREIGN DATA WRAPPER firebase_wrapper
               OPTIONS (project_id 'supa', access_token 'owner')"#,
            None,
            &[],
        )
        .unwrap();
    }

    fn create_people_table(c: &mut pgrx::spi::SpiClient, collection: &str) {
        c.update(
            &format!(
                r#"CREATE FOREIGN TABLE people (
                    name text,
                    "displayName" text,
                    age bigint,
                    score double precision,
                    active bool,
                    joined timestamp,
                    city text,
                    created_at timestamp
                  )
                  SERVER firebase_server
                  OPTIONS (
                    object 'firestore/{collection}',
                    base_url 'http://localhost:8080/v1/projects'
                  )"#
            ),
            None,
            &[],
        )
        .unwrap();
    }

    fn select_strings(c: &pgrx::spi::SpiClient, sql: &str) -> Vec<String> {
        c.select(sql, None, &[])
            .unwrap()
            .filter_map(|r| r.get::<String>(1).unwrap())
            .collect()
    }

    /// Number of the rows fetched from Firebase which Postgres removed when it
    /// re-checked the quals, which is zero when all quals are pushed down.
    fn rows_removed_by_filter(c: &pgrx::spi::SpiClient, sql: &str) -> i64 {
        let explain = format!("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) {sql}");
        select_strings(c, &explain)
            .iter()
            .find_map(|line| line.trim().strip_prefix("Rows Removed by Filter: "))
            .map(|n| n.parse().unwrap())
            .unwrap_or(0)
    }

    fn assert_pushed_down(c: &pgrx::spi::SpiClient, sql: &str) {
        let removed = rows_removed_by_filter(c, sql);
        assert_eq!(removed, 0, "Expected pushdown for [{sql}]");
    }

    fn assert_not_pushed_down(c: &pgrx::spi::SpiClient, sql: &str) {
        let removed = rows_removed_by_filter(c, sql);
        assert!(removed > 0, "Expected NO pushdown for [{sql}]");
    }

    #[pg_test]
    fn firebase_auth_users_pushdown() {
        Spi::connect_mut(|c| {
            create_server(c);
            c.update(
                r#"CREATE FOREIGN TABLE users (uid text, email text)
                   SERVER firebase_server
                   OPTIONS (
                     object 'auth/users',
                     base_url 'http://localhost:9099/identitytoolkit.googleapis.com/v1/projects'
                   )"#,
                None,
                &[],
            )
            .unwrap();

            // = on uid
            let sql = "SELECT email FROM users WHERE uid = 'JeXJ2gUHCpYSQV6zXlRYfRYHUfMl'";
            assert_eq!(select_strings(c, sql), vec!["foo@example.com"]);
            assert_pushed_down(c, sql);

            // = on email
            let sql = "SELECT email FROM users WHERE email = 'bar@example.com'";
            assert_eq!(select_strings(c, sql), vec!["bar@example.com"]);
            assert_pushed_down(c, sql);

            // IN, a missing user is left out
            let sql = "SELECT email FROM users WHERE email IN ('foo@example.com', 'x@example.com')";
            assert_eq!(select_strings(c, sql), vec!["foo@example.com"]);
            assert_pushed_down(c, sql);

            // no user found
            let sql = "SELECT email FROM users WHERE email = 'x@example.com'";
            assert!(select_strings(c, sql).is_empty());
            assert_pushed_down(c, sql);

            // other quals are re-checked by Postgres
            let sql = "SELECT email FROM users WHERE email LIKE 'foo%'";
            assert_eq!(select_strings(c, sql), vec!["foo@example.com"]);
            assert_not_pushed_down(c, sql);
        });
    }

    #[pg_test]
    fn firestore_smoketest() {
        let rt = create_async_runtime().unwrap();
        setup_firestore_people(&rt, "people_smoketest");

        Spi::connect_mut(|c| {
            create_server(c);
            create_people_table(c, "people_smoketest");

            let n: Option<i64> = c
                .select("SELECT count(*)::bigint FROM people", None, &[])
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(n, Some(5));

            let (name, created_at_set) = c
                .select(
                    r#"SELECT name, created_at IS NOT NULL FROM people
                       WHERE "displayName" = 'Alice'"#,
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get_two::<String, bool>()
                .unwrap();
            assert_eq!(
                name.as_deref(),
                Some("projects/supa/databases/(default)/documents/people_smoketest/alice")
            );
            assert_eq!(created_at_set, Some(true));

            // Missing field on dave → NULL
            let age: Option<i64> = c
                .select(
                    r#"SELECT age FROM people WHERE "displayName" = 'Dave'"#,
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(age, None);

            // Null value on carol → NULL
            let city: Option<String> = c
                .select(
                    r#"SELECT city FROM people WHERE "displayName" = 'Carol'"#,
                    None,
                    &[],
                )
                .unwrap()
                .first()
                .get(1)
                .unwrap();
            assert_eq!(city, None);
        });
    }

    #[pg_test]
    fn firestore_empty_collection() {
        let rt = create_async_runtime().unwrap();
        setup_firestore_collection(&rt, "people_empty", vec![]);

        Spi::connect_mut(|c| {
            create_server(c);
            create_people_table(c, "people_empty");

            assert!(select_strings(c, "SELECT name FROM people").is_empty());
            assert!(select_strings(c, "SELECT name FROM people WHERE age = 30").is_empty());
        });
    }

    #[pg_test]
    fn firestore_pushdown() {
        let rt = create_async_runtime().unwrap();
        setup_firestore_people(&rt, "people_pushdown");

        Spi::connect_mut(|c| {
            create_server(c);
            create_people_table(c, "people_pushdown");

            let select = |filter: &str| {
                format!(r#"SELECT "displayName" FROM people WHERE {filter} ORDER BY 1"#)
            };

            // =
            let sql = select("age = 30");
            assert_eq!(select_strings(c, &sql), vec!["Alice"]);
            assert_pushed_down(c, &sql);

            // != (excludes dave because his age is missing -> NULL is excluded by !=)
            let sql = select("age != 30");
            assert_eq!(select_strings(c, &sql), vec!["Bob", "Carol", "erin"]);
            assert_pushed_down(c, &sql);

            // < / <= / > / >=
            for (op, expected) in [
                ("<", vec!["Bob"]),
                ("<=", vec!["Alice", "Bob"]),
                (">", vec!["Carol", "erin"]),
                (">=", vec!["Alice", "Carol", "erin"]),
            ] {
                let sql = select(&format!("age {op} 30"));
                assert_eq!(select_strings(c, &sql), expected, "operator {op}");
                assert_pushed_down(c, &sql);
            }

            // IN
            let sql = select("city IN ('Amsterdam', 'Berlin')");
            assert_eq!(
                select_strings(c, &sql),
                vec!["Alice", "Bob", "Dave", "erin"]
            );
            assert_pushed_down(c, &sql);

            // NOT IN (excludes carol because her city is null)
            let sql = select("city NOT IN ('Berlin')");
            assert_eq!(select_strings(c, &sql), vec!["Alice", "Dave"]);
            assert_pushed_down(c, &sql);

            // IS NOT NULL
            let sql = select("city IS NOT NULL");
            assert_eq!(
                select_strings(c, &sql),
                vec!["Alice", "Bob", "Dave", "erin"]
            );
            assert_pushed_down(c, &sql);

            // IS NULL isn't pushed down, as Firestore doesn't match missing fields by it
            let sql = select("age IS NULL");
            assert_eq!(select_strings(c, &sql), vec!["Dave"]);
            assert_not_pushed_down(c, &sql);

            // boolean
            let sql = select("active");
            assert_eq!(select_strings(c, &sql), vec!["Alice", "Carol", "Dave"]);
            assert_pushed_down(c, &sql);
            let sql = select("NOT active");
            assert_eq!(select_strings(c, &sql), vec!["Bob", "erin"]);
            assert_pushed_down(c, &sql);

            // timestamp
            let sql = select("joined >= '2021-06-01'");
            assert_eq!(select_strings(c, &sql), vec!["Bob", "Dave", "erin"]);
            assert_pushed_down(c, &sql);

            // double, where bob's score is an integer
            let sql = select("score = 7");
            assert_eq!(select_strings(c, &sql), vec!["Bob"]);
            assert_pushed_down(c, &sql);

            // document name
            let sql = select(&format!("name = '{DOCUMENTS}/people_pushdown/bob'"));
            assert_eq!(select_strings(c, &sql), vec!["Bob"]);
            assert_pushed_down(c, &sql);
            let sql = select(&format!(
                "name IN ('{DOCUMENTS}/people_pushdown/bob', '{DOCUMENTS}/people_pushdown/x')"
            ));
            assert_eq!(select_strings(c, &sql), vec!["Bob"]);
            assert_pushed_down(c, &sql);

            // a document name in another collection can't match
            let sql = select(&format!("name = '{DOCUMENTS}/other/bob'"));
            assert!(select_strings(c, &sql).is_empty());
            assert_not_pushed_down(c, &sql);

            // multiple quals are AND'd
            let sql = select("active AND age > 30");
            assert_eq!(select_strings(c, &sql), vec!["Carol"]);
            assert_pushed_down(c, &sql);

            // Firestore allows inequality filters on one field only, the others are
            // re-checked by Postgres
            let sql = select("age > 26 AND joined > '2020-01-01'");
            assert_eq!(select_strings(c, &sql), vec!["Alice", "erin"]);
            assert_not_pushed_down(c, &sql);

            // Postgres orders text by collation and NaN above all floats, unlike
            // Firestore, so their order comparisons are re-checked by Postgres
            let sql = select(r#""displayName" > 'C'"#);
            assert_eq!(select_strings(c, &sql), vec!["Carol", "Dave", "erin"]);
            assert_not_pushed_down(c, &sql);
            let sql = select("score > 8");
            assert_eq!(select_strings(c, &sql), vec!["Alice", "Carol"]);
            assert_not_pushed_down(c, &sql);
        });
    }

    #[pg_test]
    fn firestore_pushdown_nested_collection() {
        let rt = create_async_runtime().unwrap();
        setup_firestore_collection(
            &rt,
            "parents/p1/children",
            vec![
                ("c1".to_string(), json!({ "kind": { "stringValue": "x" } })),
                ("c2".to_string(), json!({ "kind": { "stringValue": "y" } })),
            ],
        );

        Spi::connect_mut(|c| {
            create_server(c);
            c.update(
                r#"CREATE FOREIGN TABLE children (name text, kind text)
                   SERVER firebase_server
                   OPTIONS (
                     object 'firestore/parents/p1/children',
                     base_url 'http://localhost:8080/v1/projects'
                   )"#,
                None,
                &[],
            )
            .unwrap();

            let sql = "SELECT name FROM children WHERE kind = 'x'";
            assert_eq!(
                select_strings(c, sql),
                vec![format!("{DOCUMENTS}/parents/p1/children/c1")]
            );
            assert_pushed_down(c, sql);

            let sql = format!(
                "SELECT kind FROM children WHERE name = '{DOCUMENTS}/parents/p1/children/c2'"
            );
            assert_eq!(select_strings(c, &sql), vec!["y"]);
            assert_pushed_down(c, &sql);
        });
    }

    /// More documents than a page of query results, so the pages are fetched by
    /// starting after the last document of the previous page.
    #[pg_test]
    fn firestore_pushdown_paging() {
        let rt = create_async_runtime().unwrap();
        let docs = (0..1500)
            .map(|n| {
                let group = if n < 1200 { "a" } else { "b" };
                let fields = json!({
                    "n": { "integerValue": n.to_string() },
                    "group": { "stringValue": group },
                });
                (format!("doc{n:04}"), fields)
            })
            .collect();
        setup_firestore_collection(&rt, "paging", docs);

        Spi::connect_mut(|c| {
            create_server(c);
            c.update(
                r#"CREATE FOREIGN TABLE paging (name text, n bigint, "group" text)
                   SERVER firebase_server
                   OPTIONS (
                     object 'firestore/paging',
                     base_url 'http://localhost:8080/v1/projects'
                   )"#,
                None,
                &[],
            )
            .unwrap();

            for (filter, expected) in [
                ("true", 1500),
                // paged by the document name
                (r#""group" = 'a'"#, 1200),
                // paged by the inequality filter field and the document name
                ("n >= 100", 1400),
                (r#""group" = 'a' AND n >= 100"#, 1100),
            ] {
                let sql = format!(
                    "SELECT count(*)::bigint, count(DISTINCT n)::bigint FROM paging WHERE {filter}"
                );
                let (n, distinct) = c
                    .select(&sql, None, &[])
                    .unwrap()
                    .first()
                    .get_two::<i64, i64>()
                    .unwrap();
                assert_eq!((n, distinct), (Some(expected), Some(expected)), "{filter}");
                assert_pushed_down(c, &sql);
            }
        });
    }

    /// Set up a document with one field per Firestore value type read in
    /// `value_to_cell` and assert every Postgres column reads back correctly.
    #[pg_test]
    fn firestore_types_read() {
        let rt = create_async_runtime().unwrap();
        setup_firestore_collection(
            &rt,
            "types_read",
            vec![(
                "doc".to_string(),
                json!({
                    "c_bool": { "booleanValue": true },
                    "c_i2": { "integerValue": "12345" },
                    "c_i4": { "integerValue": "123456" },
                    "c_i8": { "integerValue": "9000000000" },
                    "c_f4": { "doubleValue": 1.25 },
                    "c_f8": { "doubleValue": 3.24 },
                    "c_f8_from_int": { "integerValue": "7" },
                    "c_f8_nan": { "doubleValue": "NaN" },
                    "c_text": { "stringValue": "hello" },
                    "c_varchar": { "stringValue": "world" },
                    // 1_700_000_000 s → 2023-11-14 22:13:20 UTC
                    "c_ts": { "timestampValue": "2023-11-14T22:13:20Z" },
                    "c_tstz": { "timestampValue": "2023-11-14T22:13:20Z" },
                    "c_map": { "mapValue": { "fields": { "k": { "stringValue": "v" } } } },
                    "c_null": { "nullValue": null },
                }),
            )],
        );

        Spi::connect_mut(|c| {
            create_server(c);
            c.update(
                r#"CREATE FOREIGN TABLE types_read (
                     c_bool          bool,
                     c_i2            int2,
                     c_i4            int4,
                     c_i8            int8,
                     c_f4            float4,
                     c_f8            float8,
                     c_f8_from_int   float8,
                     c_f8_nan        float8,
                     c_text          text,
                     c_varchar       varchar,
                     c_ts            timestamp,
                     c_tstz          timestamptz,
                     c_map           jsonb,
                     c_null          text,
                     c_missing       text
                   )
                   SERVER firebase_server
                   OPTIONS (
                     object 'firestore/types_read',
                     base_url 'http://localhost:8080/v1/projects'
                   )"#,
                None,
                &[],
            )
            .unwrap();

            let row = c
                .select(
                    "SELECT *, c_ts::text AS c_ts_text, extract(epoch FROM c_tstz)::int8 AS c_tstz_epoch
                     FROM types_read",
                    None,
                    &[],
                )
                .unwrap()
                .first();

            assert_eq!(row.get_by_name::<bool, _>("c_bool").unwrap(), Some(true));
            assert_eq!(row.get_by_name::<i16, _>("c_i2").unwrap(), Some(12345));
            assert_eq!(row.get_by_name::<i32, _>("c_i4").unwrap(), Some(123456));
            assert_eq!(
                row.get_by_name::<i64, _>("c_i8").unwrap(),
                Some(9_000_000_000)
            );
            assert_eq!(row.get_by_name::<f32, _>("c_f4").unwrap(), Some(1.25));
            assert_eq!(row.get_by_name::<f64, _>("c_f8").unwrap(), Some(3.24));
            assert_eq!(
                row.get_by_name::<f64, _>("c_f8_from_int").unwrap(),
                Some(7.0)
            );
            assert!(
                row.get_by_name::<f64, _>("c_f8_nan")
                    .unwrap()
                    .unwrap()
                    .is_nan()
            );
            assert_eq!(row.get_by_name::<&str, _>("c_text").unwrap(), Some("hello"));
            assert_eq!(
                row.get_by_name::<&str, _>("c_varchar").unwrap(),
                Some("world")
            );
            assert_eq!(
                row.get_by_name::<&str, _>("c_ts_text").unwrap(),
                Some("2023-11-14 22:13:20")
            );
            assert_eq!(
                row.get_by_name::<i64, _>("c_tstz_epoch").unwrap(),
                Some(1_700_000_000)
            );
            assert_eq!(
                row.get_by_name::<JsonB, _>("c_map").unwrap().map(|j| j.0),
                Some(json!({ "mapValue": { "fields": { "k": { "stringValue": "v" } } } }))
            );
            assert_eq!(row.get_by_name::<&str, _>("c_null").unwrap(), None);
            assert_eq!(row.get_by_name::<&str, _>("c_missing").unwrap(), None);
        });
    }

    #[pg_test]
    #[should_panic(expected = "column 'age' type doesn't match Firestore value")]
    fn firestore_type_mismatch() {
        let rt = create_async_runtime().unwrap();
        setup_firestore_collection(
            &rt,
            "type_mismatch",
            vec![(
                "doc".to_string(),
                json!({ "age": { "stringValue": "thirty" } }),
            )],
        );

        Spi::connect_mut(|c| {
            create_server(c);
            c.update(
                r#"CREATE FOREIGN TABLE type_mismatch (age bigint)
                   SERVER firebase_server
                   OPTIONS (
                     object 'firestore/type_mismatch',
                     base_url 'http://localhost:8080/v1/projects'
                   )"#,
                None,
                &[],
            )
            .unwrap();

            c.select("SELECT age FROM type_mismatch", None, &[])
                .unwrap();
        });
    }
}
