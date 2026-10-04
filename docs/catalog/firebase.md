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

Any other column is mapped to the top-level document field with the same name, so the fields can be queried and filtered like regular columns:

```sql
create foreign table firebase.people (
  name text,
  "displayName" text,
  age bigint,
  score double precision,
  active boolean,
  joined timestamp,
  city text
)
  server firebase_server
  options (
    object 'firestore/people'
  );

select * from firebase.people where active and city = 'Amsterdam';
```

#### Notes

- The `name`, `created_at`, and `updated_at` are automatic metadata fields on all Firestore collections
- Collection ID must be a full path ID in the format `firestore/<collection_id>`
- Examples of valid collection paths:
  - `firestore/my-collection`
  - `firestore/my-collection/my-document/another-collection`
- The `attrs` column contains all document attributes in JSON format
- Field names are case sensitive, so quote the column names of fields with upper case letters, like `"displayName"`
- A field named `name`, `fields`, `created_at`, `updated_at` or `attrs` can't be mapped to a column, use the `attrs` column for it
- A missing field or a `null` value is `NULL`, and a value which doesn't match the column type is an error. The field values are mapped to these column types:

| Firestore value type | Column type                      |
| -------------------- | -------------------------------- |
| string               | `text`, `varchar`                |
| integer              | `smallint`, `integer`, `bigint`  |
| integer, double      | `real`, `double precision`       |
| boolean              | `boolean`                        |
| timestamp            | `timestamp`, `timestamptz`       |
| any                  | `jsonb`, as the Firestore value  |

## Query Pushdown Support

This FDW supports `where` clause pushdown for `=` and `in` filters on the key columns below. Instead of listing all objects, only the matching objects are looked up in Firebase.

| Object                       | Column  | Firebase API call                                                                                                                      |
| ---------------------------- | ------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| Authentication Users         | `uid`   | [accounts:lookup](https://cloud.google.com/identity-platform/docs/reference/rest/v1/accounts/lookup)                                  |
| Authentication Users         | `email` | [accounts:lookup](https://cloud.google.com/identity-platform/docs/reference/rest/v1/accounts/lookup)                                  |
| Firestore Database Documents | `name`  | [documents:batchGet](https://firebase.google.com/docs/firestore/reference/rest/v1beta1/projects.databases.documents/batchGet)         |

For example, this query

```sql
select * from firebase.users where email = 'foo@example.com';
```

will be translated to a single Firebase API call `POST https://identitytoolkit.googleapis.com/v1/projects/<project_id>/accounts:lookup` with request body `{"email": ["foo@example.com"]}`. An `in` filter with more than 100 values is not pushed down.

### Firestore document fields

Filters on the columns mapped to document fields are pushed down to a Firestore [query](https://firebase.google.com/docs/firestore/reference/rest/v1beta1/projects.databases.documents/runQuery), so only the matching documents are fetched.

| Operator                 | Column types                                                   |
| ------------------------ | -------------------------------------------------------------- |
| `=`, `in`                | `text`, `boolean`, integer, float and timestamp types           |
| `<`, `<=`, `>`, `>=`     | integer and timestamp types                                    |

Firestore needs a [composite index](https://firebase.google.com/docs/firestore/query-data/index-overview) to combine equality filters with range filters, or range filters on different fields. So that no composite index is needed, either the equality filters on any fields are pushed down, or else the range filters on a single field. For example, in this query only `active = true` is pushed down, and `age > 30` is applied in Postgres:

```sql
select * from firebase.people where active and age > 30;
```

Range filters on `text` and float columns are not pushed down, as Postgres compares them differently than Firestore (by collation, and with `NaN` above all numbers). An `in` filter on more than 30 values is not pushed down either.

### Not pushed down

Other filters, including filters on the `attrs` column, `order by` and `limit` are not pushed down, they are applied in Postgres after the objects are fetched.

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
