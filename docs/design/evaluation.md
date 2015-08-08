## evaluation order

design doc for SQL statement evaluation

### precedence model for doing where-filtering
These actions apply to select, update, delete:
1. CTEs
1. nested select
  - can this bind variables from its parent scope? check specs, but easier not to
1. single-table where
1. multi-table where

When the expression is a select, we're also interested in:
1. order & group
2. create output rows
