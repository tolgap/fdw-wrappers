#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::JsonB;
    use pgrx::prelude::*;

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

            c.update(
                r#"
                CREATE FOREIGN TABLE firebase_docs_empty (
                  name text,
                  attrs jsonb
                )
                SERVER my_firebase_server
                OPTIONS (
                  object 'firestore/empty-collection',
                  base_url 'http://localhost:8080/v1/projects'
                )
             "#,
                None,
                &[],
            )
            .unwrap();

            let results = c
                .select("SELECT name FROM firebase_docs_empty", None, &[])
                .unwrap()
                .collect::<Vec<_>>();
            assert!(results.is_empty());

            // Rows fetched from Firebase but removed by the local filter, which
            // is zero when the quals are pushed down
            macro_rules! rows_removed_by_filter {
                ($sql:expr) => {{
                    let explain = format!(
                        "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) {}",
                        $sql
                    );
                    c.select(&explain, None, &[])
                        .unwrap()
                        .filter_map(|r| r.get::<&str>(1).unwrap().map(|s| s.to_string()))
                        .find_map(|line| {
                            line.trim()
                                .strip_prefix("Rows Removed by Filter: ")
                                .map(|n| n.parse::<i64>().unwrap())
                        })
                        .unwrap_or(0)
                }};
            }

            macro_rules! select_column {
                ($sql:expr, $col:expr) => {{
                    c.select($sql, None, &[])
                        .unwrap()
                        .filter_map(|r| r.get_by_name::<&str, _>($col).unwrap())
                        .collect::<Vec<_>>()
                }};
            }

            // auth users lookup by uid
            let sql = "SELECT email FROM firebase_users WHERE uid = 'JeXJ2gUHCpYSQV6zXlRYfRYHUfMl'";
            assert_eq!(select_column!(sql, "email"), vec!["foo@example.com"]);
            assert_eq!(rows_removed_by_filter!(sql), 0);

            // auth users lookup by email
            let sql = "SELECT email FROM firebase_users WHERE email = 'bar@example.com'";
            assert_eq!(select_column!(sql, "email"), vec!["bar@example.com"]);
            assert_eq!(rows_removed_by_filter!(sql), 0);

            let sql = "SELECT email FROM firebase_users
                       WHERE email IN ('foo@example.com', 'missing@example.com')";
            assert_eq!(select_column!(sql, "email"), vec!["foo@example.com"]);
            assert_eq!(rows_removed_by_filter!(sql), 0);

            let sql = "SELECT email FROM firebase_users WHERE email = 'missing@example.com'";
            assert!(select_column!(sql, "email").is_empty());
            assert_eq!(rows_removed_by_filter!(sql), 0);

            // quals which can't be pushed down are still filtered locally
            let sql = "SELECT email FROM firebase_users WHERE email LIKE 'foo%'";
            assert_eq!(select_column!(sql, "email"), vec!["foo@example.com"]);
            assert_eq!(rows_removed_by_filter!(sql), 1);

            // firestore documents lookup by name
            let sql = "SELECT name FROM firebase_docs WHERE name =
                       'projects/supa/databases/(default)/documents/my-collection/bSMScXpZHMJe9ilE9Yqs'";
            assert_eq!(
                select_column!(sql, "name"),
                vec![
                    "projects/supa/databases/(default)/documents/my-collection/bSMScXpZHMJe9ilE9Yqs"
                ]
            );
            assert_eq!(rows_removed_by_filter!(sql), 0);

            let sql = "SELECT name FROM firebase_docs WHERE name =
                       'projects/supa/databases/(default)/documents/my-collection/missing'";
            assert!(select_column!(sql, "name").is_empty());
            assert_eq!(rows_removed_by_filter!(sql), 0);

            // a document in a nested collection is not in the parent collection
            let sql = "SELECT name FROM firebase_docs WHERE name =
                       'projects/supa/databases/(default)/documents/my-collection/bSMScXpZHMJe9ilE9Yqs/my-collection2/fkSWL4hNJ3lRc1ZIorPm'";
            assert!(select_column!(sql, "name").is_empty());
            assert_eq!(rows_removed_by_filter!(sql), 0);

            let sql = "SELECT name FROM firebase_docs_nested WHERE name IN (
                       'projects/supa/databases/(default)/documents/my-collection/bSMScXpZHMJe9ilE9Yqs/my-collection2/fkSWL4hNJ3lRc1ZIorPm',
                       'projects/supa/databases/(default)/documents/my-collection/bSMScXpZHMJe9ilE9Yqs/my-collection2/missing')";
            assert_eq!(
                select_column!(sql, "name"),
                vec![
                    "projects/supa/databases/(default)/documents/my-collection/bSMScXpZHMJe9ilE9Yqs/my-collection2/fkSWL4hNJ3lRc1ZIorPm"
                ]
            );
            assert_eq!(rows_removed_by_filter!(sql), 0);

            c.update(
                r#"
                CREATE FOREIGN TABLE firebase_people (
                  name text,
                  "displayName" text,
                  age bigint,
                  score double precision,
                  active boolean,
                  joined timestamp,
                  city text,
                  attrs jsonb
                )
                SERVER my_firebase_server
                OPTIONS (
                  object 'firestore/people',
                  base_url 'http://localhost:8080/v1/projects'
                )
             "#,
                None,
                &[],
            )
            .unwrap();

            // the other columns are mapped to the document fields with the same name,
            // a missing field (Dave's age) and a null value (Carol's city) are NULL
            let results = c
                .select(
                    r#"SELECT "displayName", age, score, active, city, joined::text
                       FROM firebase_people ORDER BY "displayName""#,
                    None,
                    &[],
                )
                .unwrap()
                .map(|r| {
                    (
                        r.get_by_name::<&str, _>("displayName").unwrap().unwrap(),
                        r.get_by_name::<i64, _>("age").unwrap(),
                        r.get_by_name::<f64, _>("score").unwrap().unwrap(),
                        r.get_by_name::<bool, _>("active").unwrap().unwrap(),
                        r.get_by_name::<&str, _>("city").unwrap(),
                        r.get_by_name::<&str, _>("joined").unwrap().unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(
                results,
                vec![
                    (
                        "Alice",
                        Some(30),
                        9.5,
                        true,
                        Some("Amsterdam"),
                        "2020-01-15 10:00:00"
                    ),
                    (
                        "Bob",
                        Some(25),
                        7.0,
                        false,
                        Some("Berlin"),
                        "2021-06-01 00:00:00"
                    ),
                    ("Carol", Some(35), 8.25, true, None, "2019-03-10 08:30:00"),
                    (
                        "Dave",
                        None,
                        6.5,
                        true,
                        Some("Amsterdam"),
                        "2022-11-20 12:00:00"
                    ),
                    (
                        "erin",
                        Some(41),
                        7.75,
                        false,
                        Some("Berlin"),
                        "2023-08-05 16:45:00"
                    ),
                ]
            );

            // filters on the document fields, with the people they match and the number
            // of rows removed by the local filter, which is zero when fully pushed down
            let cases: &[(&str, &[&str], i64)] = &[
                (r#""displayName" = 'Bob'"#, &["Bob"], 0),
                ("city = 'Amsterdam'", &["Alice", "Dave"], 0),
                ("active", &["Alice", "Carol", "Dave"], 0),
                ("NOT active", &["Bob", "erin"], 0),
                ("active AND city = 'Amsterdam'", &["Alice", "Dave"], 0),
                (
                    "city IN ('Amsterdam', 'Berlin')",
                    &["Alice", "Bob", "Dave", "erin"],
                    0,
                ),
                ("age IN (25, 41)", &["Bob", "erin"], 0),
                ("age = 99", &[], 0),
                ("age > 30", &["Carol", "erin"], 0),
                ("age >= 30 AND age < 41", &["Alice", "Carol"], 0),
                ("joined >= '2021-06-01'", &["Bob", "Dave", "erin"], 0),
                // Bob's score is stored as an integer
                ("score = 7", &["Bob"], 0),
                // only the equality filters are pushed down, as combining them with a
                // range filter on another field needs a composite index in Firestore
                ("active AND age > 30", &["Carol"], 2),
                // range filters on text and floats are left to Postgres, as it orders
                // them differently (collations and NaN)
                (r#""displayName" > 'C'"#, &["Carol", "Dave", "erin"], 2),
                ("score > 8", &["Alice", "Carol"], 3),
                ("city IS NULL", &["Carol"], 4),
            ];
            for (filter, expected, removed) in cases {
                let sql = format!(
                    r#"SELECT "displayName" FROM firebase_people WHERE {filter} ORDER BY 1"#
                );
                // Postgres doesn't push filters down into a subquery with an offset,
                // so this gets the same result by filtering all documents locally
                let reference = format!(
                    r#"SELECT "displayName" FROM (SELECT * FROM firebase_people OFFSET 0) t
                       WHERE {filter} ORDER BY 1"#
                );
                assert_eq!(
                    select_column!(&sql, "displayName"),
                    *expected,
                    "filter: {filter}"
                );
                assert_eq!(
                    select_column!(&reference, "displayName"),
                    *expected,
                    "reference: {filter}"
                );
                assert_eq!(rows_removed_by_filter!(sql), *removed, "removed: {filter}");
            }
        });
    }
}
