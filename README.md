# Fivetran's MySQL Secure Agent

# Anonymizing columns

You can configure the secure agent to anonymize sensitive data by hashing:

```json
{
  "schemas": {
    "my_schema": {
      "tables": {
        "my_table": {
          "columns": {
            "ssn": {
              "hash": true
            }
          }
        }
      }
    }
  },
  "crypto": {
    "salt": "abc123"
  }
}
```

Fivetran will append `crypto.salt` to each value before hashing it to protect against [dictionary attacks](https://en.wikipedia.org/wiki/Dictionary_attack).