# Tree configuration
configurations can be formated as `.yml` or `.toml`.

`name` (str): Tree name

`type` (tree | forest): Tree type

`data` (dict[str, Any] | None) optional
* `path` (str): File path to a `jsonlines` (`.jl`) file containing the nodes data.
* `keymap` (dict[str, str] | None) optional: Translation key values between nodes keys and weetags keys. Default `None`

`uri` (dict[str, Any]):
* `dialect` (sqlite | postgresql). default `sqlite`
* `driver` (psycopg | None). default `None`. When dialect == `postgres`, set `driver` to `psycopg`
* `database` (str): database file path (sqlite) or database name (psql). default `:memory:`
* `port` (int) optional. Database port
* `host` (str) optional. Database host
* `username` (str) optional. Database username
* `password` (str) optional. Database password

`cache` (dict[str, Any] | None) optional:
* `cache` (local | memcached): define cache system to use
* `opts` (dict[str, Any] | None) optional.ache configuration. Keys are depending of the `cache` selected:
  * `server` (str): `<host>:<port>` of the `memcached` server (memcached only)
  * `workers` (int): number of worker of the `memcached` server (memcached only). Default 4.
  * `prefix` (str): prefix used by the `memcached` server (memcached only). Default `weetags_`

`builder` (dict[str, Any]): configurate the TreeBuilder process:
* `not_exist` (bool) optional: When True, Building process is oly done if the tree doesn't already exist. Default `False`
* `recreate` (bool) optional: When True, Drop existing and restart tree from scratch. Default `False`
* `on_change` (`raise`, `ignore`, `alter`, `recreate`) optional: Tree alteration strategy. Plan how the builder handle structural difference between imported data and existing tables. Default `raise`
* `on_collision` (`raise`, `ignore`, `update`, `replace`) optional: Imported data collision strategy. Define how the builder process handle importation of nodes that already exist in the tree. Default `raise`
* `skip_init` (bool) optional: Skip the initialization process. The builder process do not create the Tree tables. If `True` and the tables do not already exist, the builder process will fail. Default `False`
* `skip_import` (bool) optional: Skip importation step. The builder process do not insert data inside the tree. Default `False`

`structure` (dict[str, Any]): 
* `fields` (list[FieldDefinitions]): Define and configure the Tree Metadata structure by listing all the Field Definitions. Field Definition configuration are defined [here](#field-definition-configuration) 
* `indexes` (list[list[str]] | None) optional: Define sets of indexes. Default `None`
* `unique_constraints` (list[list[str]] | None) optional: Define sets of unique constraints. Default `None`

## Field Definition Configuration

* `name` (str): field name
* `type` (`text`, `integer`, `boolean`, `json`, `datetime`): sql type of the field
* `unique` (bool) optional: apply unique constraint on the field. Default `False`
* `index` (bool) optional: create an index for the field. Default `False`
* `nullable` (bool) optional: is the field nullable. Default `False`
