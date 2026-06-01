# Databricks notebook source

# COMMAND ----------

# DBTITLE 1,Shared Utilities
import re

def validate_identifier(name, identifier_type="identifier"):
    """
    Validate a Unity Catalog identifier for SQL injection safety.
    Only allows non-delimited identifiers — no backticks, hyphens, or special chars.
    Reference: https://docs.databricks.com/sql/language-manual/sql-ref-identifiers.html
    """
    if not name:
        raise ValueError(f"{identifier_type} cannot be empty")
    if len(name) > 255:
        raise ValueError(f"{identifier_type} too long: {len(name)} chars (max 255)")
    if not re.match(r'^[a-zA-Z_]', name):
        raise ValueError(
            f"Invalid {identifier_type}: '{name}'. "
            f"Must start with a letter (A-Z, a-z) or underscore (_)."
        )
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
        raise ValueError(
            f"Invalid {identifier_type}: '{name}'. "
            f"Can only contain letters, digits, and underscores."
        )
    return name

print("✅ valhalla_utils loaded")
