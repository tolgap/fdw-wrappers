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

            macro_rules! select_emails {
                ($sql:expr) => {{
                    c.select($sql, None, &[])
                        .unwrap()
                        .filter_map(|r| r.get_by_name::<&str, _>("email").unwrap())
                        .collect::<Vec<_>>()
                }};
            }

            macro_rules! select_names {
                ($sql:expr) => {{
                    c.select($sql, None, &[])
                        .unwrap()
                        .filter_map(|r| r.get_by_name::<&str, _>("name").unwrap())
                        .collect::<Vec<_>>()
                }};
            }

            // auth users lookup by uid
            let sql = "SELECT email FROM firebase_users WHERE uid = 'JeXJ2gUHCpYSQV6zXlRYfRYHUfMl'";
            assert_eq!(select_emails!(sql), vec!["foo@example.com"]);
            assert_eq!(rows_removed_by_filter!(sql), 0);

            // auth users lookup by email
            let sql = "SELECT email FROM firebase_users WHERE email = 'bar@example.com'";
            assert_eq!(select_emails!(sql), vec!["bar@example.com"]);
            assert_eq!(rows_removed_by_filter!(sql), 0);

            let sql = "SELECT email FROM firebase_users
                       WHERE email IN ('foo@example.com', 'missing@example.com')";
            assert_eq!(select_emails!(sql), vec!["foo@example.com"]);
            assert_eq!(rows_removed_by_filter!(sql), 0);

            let sql = "SELECT email FROM firebase_users WHERE email = 'missing@example.com'";
            assert!(select_emails!(sql).is_empty());
            assert_eq!(rows_removed_by_filter!(sql), 0);

            // quals which can't be pushed down are still filtered locally
            let sql = "SELECT email FROM firebase_users WHERE email LIKE 'foo%'";
            assert_eq!(select_emails!(sql), vec!["foo@example.com"]);
            assert_eq!(rows_removed_by_filter!(sql), 1);

            // firestore documents lookup by name
            let sql = "SELECT name FROM firebase_docs WHERE name =
                       'projects/supa/databases/(default)/documents/my-collection/bSMScXpZHMJe9ilE9Yqs'";
            assert_eq!(
                select_names!(sql),
                vec![
                    "projects/supa/databases/(default)/documents/my-collection/bSMScXpZHMJe9ilE9Yqs"
                ]
            );
            assert_eq!(rows_removed_by_filter!(sql), 0);

            let sql = "SELECT name FROM firebase_docs WHERE name =
                       'projects/supa/databases/(default)/documents/my-collection/missing'";
            assert!(select_names!(sql).is_empty());
            assert_eq!(rows_removed_by_filter!(sql), 0);

            // a document in a nested collection is not in the parent collection
            let sql = "SELECT name FROM firebase_docs WHERE name =
                       'projects/supa/databases/(default)/documents/my-collection/bSMScXpZHMJe9ilE9Yqs/my-collection2/fkSWL4hNJ3lRc1ZIorPm'";
            assert!(select_names!(sql).is_empty());
            assert_eq!(rows_removed_by_filter!(sql), 0);

            let sql = "SELECT name FROM firebase_docs_nested WHERE name IN (
                       'projects/supa/databases/(default)/documents/my-collection/bSMScXpZHMJe9ilE9Yqs/my-collection2/fkSWL4hNJ3lRc1ZIorPm',
                       'projects/supa/databases/(default)/documents/my-collection/bSMScXpZHMJe9ilE9Yqs/my-collection2/missing')";
            assert_eq!(
                select_names!(sql),
                vec![
                    "projects/supa/databases/(default)/documents/my-collection/bSMScXpZHMJe9ilE9Yqs/my-collection2/fkSWL4hNJ3lRc1ZIorPm"
                ]
            );
            assert_eq!(rows_removed_by_filter!(sql), 0);
        });
    }
}
