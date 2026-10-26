# Server configuration
configurations can be formated as `.yml` or `.toml`.

`name` (str): server name

`trees` (path): folder or file path to a [tree specs file](/docs/specs_files.md)

`api` (dict[str, bool]): define which api is enabled or not.
* `v1` (bool): enable v1 api. Default: `True` 

`configs` (dict[str, Any]) Optional: define server configuration
* `secret_key` (str): sanic secret key.

`auth` (dict[str, Any]) Optional: authentication configuration. Authentication system delivers JWT tokens:
* `max_age` (int): maximum age of a delivered JWT token in seconds.
* `secret_key` (str): secret key used to encrypt/decrypt JWT tokens 
* `maximum_priority` (int): maximum priority a rule can be given. default 1000. 
* `builder` (dict[str, Any]): configure authentication builder process
  * `recreate_rules` (bool): When true, delete existing authentication rules before importing new configurated `rules`.
  * `default` (`deny` | `allow`): default authenticator behavior. When set to  `deny`, all request are denied outside of the `login` blueprint. You must define some `rules` to provide access to weetags trees
* `uri` (dict[str, Any]):
  * `dialect` (sqlite | postgresql). default `sqlite`
  * `driver` (psycopg | None). default `None`. When dialect == `postgres`, set `driver` to `psycopg`
  * `database` (str): database file path (sqlite) or database name (psql). default `:memory:`
  * `port` (int) optional. Database port
  * `host` (str) optional. Database host
  * `username` (str) optional. Database username
  * `password` (str) optional. Database password
* `rules` (list[Rules]): list and order all authentication rules applied to the server paths. `Rules` configuration is are defined [here](#authentication-rules)

## Authentication rules
* `type` (path | blueprint): define the rule type
* `priotiry` (int): define the rule priority. The closest to 0 the more priority is given to a rule.
* `methods`: (list[str]): define the list of http methods (`GET`, `POST`, `PUT`, `PATCH`) the rule encompass
* `rule` (str): depending on the rule `type` value, `rule` is either a `blueprint name` or an `endpoint path.`
  * `methods` argument is not taken into account when it is a `blueprint` rule.
  * `paths` are `glob` compatible, which means `*` wildcards are usable to cover larger path spectrum with one rule. 
* `role` (str | None | *): the role attributed to the rule.
  * `role` == `None` implies that the path is forbidden for anyone.
  * `role` == `*` implies that the path is accessible for anyone, even unauthenticated users.
  * `role` == `<your-role-name>` implies that the path is only accessible to users with the `<your-role-name>` role.

## Users 
Users are defined by a `username`, `password`, and `roles`. Roles are a list of role names, that associated with `rules`.


Users can be added/updated/removed using `wee` cli. To use the cli, you will be required to have a server configuration file with `auth.uri` keychain being configurated. By default, Weetags look from `server.yml` file, but this can be modified using `-c <configs/path>` with your commands.

**add a new user**: `wee users add <username> -r <role-name> -r <other-role-name>`. Which will then prompt you to enter a `password`.

**remove a user**: `wee users rm <username>`

**update user password**: `wee users update password <username>`. You will get prompted to verify and modify your `password`.

**update user roles**: `wee users update roles <username> -r <role-name> -r <other-role-name>`

