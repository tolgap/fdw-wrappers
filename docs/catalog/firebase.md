---
source:
documentation:
author: supabase
tags:
  - native
  - official
---

# Firebase

[Firebase](https://firebase.google.com/) is an app development platform built around non-relational technologies. The Firebase Wrapper supports connecting to below objects.

1. [Authentication Users](https://firebase.google.com/docs/auth/users) (_read only_)
2. [Firestore Database Documents](https://firebase.google.com/docs/firestore) (_read only_)

## Preparation

Before you can query Firebase, you need to enable the Wrappers extension and store your credentials in Postgres.

### Enable Wrappers

Make sure the `wrappers` extension is installed on your database:

```sql
create extension if not exists wrappers with schema extensions;
```

### Enable the Firebase Wrapper

Enable the `firebase_wrapper` FDW:

```sql
create foreign data wrapper firebase_wrapper
  handler firebase_fdw_handler
  validator firebase_fdw_validator;
```

### Store your credentials (optional)

By default, Postgres stores FDW credentials inside `pg_catalog.pg_foreign_server` in plain text. Anyone with access to this table will be able to view these credentials. Wrappers is designed to work with [Vault](https://supabase.com/docs/guides/database/vault), which provides an additional level of security for storing credentials. We recommend using Vault to store your credentials.

```sql
-- Save your Firebase credentials in Vault and retrieve the created `key_id`
select vault.create_secret(
  '{
      "type": "service_account",
      "project_id": "your_gcp_project_id",
      ...
  }',
  'firebase',
  'Firebase API key for Wrappers'
);
```

### Connecting to Firebase

We need to provide Postgres with the credentials to connect to Firebase, and any additional options. We can do this using the `create server` command:

=== "With Vault"

    ```sql
    create server firebase_server
      foreign data wrapper firebase_wrapper
      options (
        sa_key_id '<key_ID>', -- The Key ID from above.
        project_id '<firebase_project_id>'
    );
    ```

=== "Without Vault"

    ```sql
    create server firebase_server
      foreign data wrapper firebase_wrapper
       options (
         sa_key '
         {
            "type": "service_account",
            "project_id": "your_gcp_project_id",
            ...
         }
        ',
         project_id 'firebase_project_id'
       );
    ```

### Create a schema

We recommend creating a schema to hold all the foreign tables:

```sql
create schema if not exists firebase;
```

## Options

The full list of foreign table options are below:

- `object` - Object name in Firebase, required.

  For Authenciation users, the object name is fixed to `auth/users`. For Firestore documents, its format is `firestore/<collection_id>`, note that collection id must be a full path id. For example,

  - `firestore/my-collection`
  - `firestore/my-collection/my-document/another-collection`


## Entities

### Authentication Users

This is an object representing Firebase Authentication Users.

Ref: [Firebase Authentication Users](https://firebase.google.com/docs/auth/users)

#### Operations

| Object               | Select | Insert | Update | Delete | Truncate |
| -------------------- | :----: | :----: | :----: | :----: | :------: |
| Authentication Users |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table firebase.users (
  uid text,
  email text,
  created_at timestamp,
  attrs jsonb
)
  server firebase_server
  options (
    object 'auth/users'
  );
```

#### Notes

- The `attrs` column contains all user attributes in JSON format
- This is a special collection with unique metadata fields

### Firestore Database Documents

This is an object representing Firestore Database Documents.

Ref: [Firestore Database](https://firebase.google.com/docs/firestore)

#### Operations

| Object                       | Select | Insert | Update | Delete | Truncate |
| ---------------------------- | :----: | :----: | :----: | :----: | :------: |
| Firestore Database Documents |   ✅    |   ❌    |   ❌    |   ❌    |    ❌     |

#### Usage

```sql
create foreign table firebase.docs (
  name text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server firebase_server
  options (
    object 'firestore/user-profiles'
  );
```

Each other column maps to a top-level document field of the same name, see [Schema Mapping](#schema-mapping):

```sql
create foreign table firebase.people (
  name text,
  "displayName" text,
  age bigint,
  active boolean,
  joined timestamp,
  city text
)
  server firebase_server
  options (
    object 'firestore/people'
  );
```

#### Notes

- The `name`, `created_at`, and `updated_at` are automatic metadata fields on all Firestore collections
- Collection ID must be a full path ID in the format `firestore/<collection_id>`
- Examples of valid collection paths:
  - `firestore/my-collection`
  - `firestore/my-collection/my-document/another-collection`
- The `attrs` column contains all document attributes in JSON format

## Schema Mapping

Each column declared on a Firestore foreign table, other than the `name`, `fields`, `created_at`, `updated_at` and `attrs` columns, maps to a top-level document field of the same name (exact match):

- Field names are case sensitive, so quote the column names of fields with upper case letters, like `"displayName"`.
- If a document does not contain a field, or its value is `null`, the corresponding column is set to `NULL`.
- Dots in column names are treated as literal characters — they do not traverse maps. Use the `attrs` column for nested field access, and for a field named like one of the columns above.
- When an `attrs` or `fields` column is declared, the full documents are fetched from Firestore, otherwise only the fields of the declared columns.

## Query Pushdown Support

### Authentication Users

`=` and `in` filters on the `uid` and `email` columns are pushed down to an [accounts:lookup](https://cloud.google.com/identity-platform/docs/reference/rest/v1/accounts/lookup) call, so only the matching users are fetched. For example, this query

```sql
select * from firebase.users where email = 'foo@example.com';
```

will be translated to a single Firebase API call `POST https://identitytoolkit.googleapis.com/v1/projects/<project_id>/accounts:lookup` with request body `{"email": ["foo@example.com"]}`. An `in` filter with more than 100 values is not pushed down.

### Firestore Database Documents

This FDW supports `where` clause pushdown, the filters are sent in a Firestore [query](https://firebase.google.com/docs/firestore/reference/rest/v1beta1/projects.databases.documents/runQuery).

#### Supported Operators

The following SQL predicates are translated to Firestore filter operators:

| SQL predicate     | Firestore filter          |
| ----------------- | ------------------------- |
| `=`               | `EQUAL`                   |
| `!=`              | `NOT_EQUAL`               |
| `<`               | `LESS_THAN`               |
| `<=`              | `LESS_THAN_OR_EQUAL`      |
| `>`               | `GREATER_THAN`            |
| `>=`              | `GREATER_THAN_OR_EQUAL`   |
| `IN (...)`        | `IN`                      |
| `NOT IN (...)`    | `NOT_IN`                  |
| `IS NOT NULL`     | `IS_NOT_NULL`             |

Multiple `where` predicates are AND'd in a composite filter. The `name` column is filtered as the document `__name__`, by `=` and `IN (...)` only. Any predicate that is not supported is omitted from the Firestore filter and re-checked by Postgres after the documents are returned, so the result is always correct. These predicates are not pushed down:

- `IS NULL`, as Firestore doesn't match missing fields by it
- `<`, `<=`, `>` and `>=` on `text` and float columns, as Postgres orders text by collation and `NaN` above all numbers, unlike Firestore
- The predicates which Firestore can't [combine in one query](https://firebase.google.com/docs/firestore/query-data/queries#limitations) with the others: inequality filters (`!=`, `<`, `<=`, `>`, `>=`, `NOT IN` and `IS NOT NULL`) on more than one field, more than one `!=`, `NOT IN` or `IS NOT NULL`, more than one `IN` or `NOT IN`, and an `IN` with more than 30 or `NOT IN` with more than 10 values
- Predicates on the `fields`, `created_at`, `updated_at` and `attrs` columns

`order by` and `limit` are not pushed down, because Postgres also passes the `limit` when a predicate isn't pushed down, like a predicate on the `attrs` column, so fetching fewer documents could leave out matching ones.

!!! note

    Firestore needs a [composite index](https://firebase.google.com/docs/firestore/query-data/index-overview#composite_indexes) to combine an inequality filter with a filter on another field, like `where active and age > 30`. Without the index, the query fails with an error containing a link to create it.

## Supported Data Types

| Firestore Type | Postgres Type                       | Notes                                           |
| -------------- | ----------------------------------- | ----------------------------------------------- |
| boolean        | bool                                |                                                 |
| integer        | int2 / int4 / int8 / float4 / float8 |                                                 |
| double         | float4 / float8                     |                                                 |
| string         | text / varchar                      |                                                 |
| timestamp      | timestamp / timestamptz             |                                                 |
| any            | jsonb                               | The Firestore value, like `{"mapValue": {...}}` |
| null / missing | any                                 | Column is set to `NULL`                         |

A value which doesn't match the column type is an error.

## Limitations

This section describes important limitations and considerations when using this FDW:

- Only support read-only access to Authentication Users and Firestore Database Documents
- Default maximum row count limit is 10,000 records
- Full result sets are loaded into memory, which can impact PostgreSQL performance with large datasets
- Materialized views using these foreign tables may fail during logical backups

## Examples

Some examples on how to use Firebase foreign tables.

### firestore

To map a Firestore collection provide its location using the format `firestore/<collection_id>` as the `object` option as shown below.

```sql
create foreign table firebase.docs (
  name text,
  created_at timestamp,
  updated_at timestamp,
  attrs jsonb
)
  server firebase_server
  options (
    object 'firestore/user-profiles'
  );
```

Note that `name`, `created_at`, and `updated_at`, are automatic metadata fields on all Firestore collections.

### auth/users

The `auth/users` collection is a special case with unique metadata. The following shows how to map Firebase users to PostgreSQL table.

```sql
create foreign table firebase.users (
  uid text,
  email text,
  created_at timestamp,
  attrs jsonb
)
  server firebase_server
  options (
    object 'auth/users'
  );
```
