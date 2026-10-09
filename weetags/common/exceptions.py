

from email import message
from typing import Sequence


class WeetagsException(Exception):
    status_code: int

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class AuthenticatorExeption(WeetagsException):
    ...

class ForbiddenAccessError(AuthenticatorExeption):
    status_code: int = 403
    message = "Access Forbidden."

    def __init__(self) -> None:
        super().__init__(ForbiddenAccessError.message)

class UnauthorizedAccessError(AuthenticatorExeption):
    status_code: int = 401

    def __init__(self, *args: object) -> None:
        super().__init__(*args)




"""
TreeError:

    TooManyRootError
    AlreadyHasRootError
    NoRootError


    NodeError

    FieldError:
        ReservedFieldNameError
        ExistingFieldNameError
        FieldConstrainError

    TreeEngineError:
        TreeEngineDialectError

ConfigsError:
    ParsedTypeError
    LiteralError

ArgumentError:
    UnknownArgumentError

NodePathError:
    PathSegmentationError
    NodeNotInPathError
    NodeNotInPathsError





PathUtils
TreeError>TreePathError:
    PathSegmentationError: seperator, size, last node must be excluded, first must be included
    NotInPathError: name not in path
    ValidatationError
"""



class TreeError(WeetagsException):
    ...

class NodeError(TreeError):
    status_code: int = 400
    message = "Unkown node {key}: {name}"

    def __init__(self, key: str, name: str) -> None:
        super().__init__(NodeError.message.format(key=key, name=name))

class FieldError(TreeError):
    status_code: int = 400
    message = "Unkown field: {name}"

    def __init__(self, name: str) -> None:
        super().__init__(FieldError.message.format(name=name))

class ReservedFieldNameError(FieldError):
    status_code: int = 400
    message = "field `{name}` is a reserved namespace"

    def __init__(self, name: str) -> None:
        super().__init__(ReservedFieldNameError.message.format(name=name))

class ExistingFieldNameError(FieldError):
    status_code: int = 400
    message = "field `{name}` already exist"

    def __init__(self, name: str) -> None:
        super().__init__(ExistingFieldNameError.message.format(name=name))

class FieldConstrainError(FieldError):
    status_code: int = 400

    def __init__(self, *args) -> None:
        super().__init__(*args)






class TreeEngineError(TreeError):
    ...

class TreeEngineDialectError(TreeEngineError):
    status_code: int = 400

    def __init__(self, *args) -> None:
        super().__init__(*args)    

class TableError(TreeEngineError):
    status_code: int = 400

    def __init__(self, *args) -> None:
        super().__init__(*args)

class SchemaError(TreeEngineError):
    status_code: int = 400
    message = "Schema error: {name}"

    def __init__(self, name: str) -> None:
        super().__init__(SchemaError.message.format(name=name))




class TooManyRootError(TreeError):
    status_code: int = 400
    message = "{tree} tree has too many Root Candidates"

    def __init__(self, tree: str) -> None:
        super().__init__(TooManyRootError.message.format(tree=tree))

class AlreadyHasRootError(TreeError):
    status_code: int = 400
    message = "{tree} tree has already a root."

    def __init__(self, tree: str) -> None:
        super().__init__(AlreadyHasRootError.message.format(tree=tree))

class NoRootError(TreeError):
    status_code: int = 400
    message = "{tree} tree has no root"

    def __init__(self, tree: str) -> None:
        super().__init__(NoRootError.message.format(tree=tree))





# CONFIGS / PARSING / VALIDATION
class ConfigsError(WeetagsException):
    status_code: int = 400

    def __init__(self, *args) -> None:
        super().__init__(*args)


class ParsedTypeError(ConfigsError):
    status_code: int = 400
    message = "{key} must be of type: {expected_type}"

    def __init__(self, key: str, expected_type: str) -> None:
        super().__init__(ParsedTypeError.message.format(key=key, expected_type=expected_type))

class LiteralError(ConfigsError):
    status_code: int = 400
    message = "{key} must be one of: {literal}"

    def __init__(self, key: str, literal: Sequence[str]) -> None:
        super().__init__(LiteralError.message.format(key=key, literal=literal))

class ValidatationError(ConfigsError):
    status_code: int = 400
    message = "{key} must be of type {expected_type}"

    def __init__(self, key: str, expected_type: str) -> None:
        super().__init__(ValidatationError.message.format(key=key, expected_type=expected_type))

# ARGUMENT / ARGUMENT PARSING / VALIDATION
class ArgumentError(WeetagsException):
    status_code: int = 400

    def __init__(self, *args) -> None:
        super().__init__(*args)

class UnknownArgumentError(ConfigsError):
    status_code: int = 400
    message = "Unknown argument: {name}"

    def __init__(self, name: str) -> None:
        super().__init__(UnknownArgumentError.message.format(name=name))


# PATHUTILS


class NodePathError(WeetagsException):
    status_code: int = 400

    def __init__(self, *args) -> None:
        super().__init__(*args)

class PathSegmentationError(NodePathError):
    status_code: int = 400

    def __init__(self, *args) -> None:
        super().__init__(*args)

class NodeNotInPathError(NodePathError):
    status_code: int = 400
    message: str = "`{name}` is not part of path `{path}`"

    def __init__(self, name: str, path: str) -> None:
        super().__init__(NodeNotInPathError.message.format(name=name, path=path))

class NodeNotInPathsError(NodePathError):
    status_code: int = 400
    message: str = "`{name}` is not in the path collection"

    def __init__(self, name: str) -> None:
        super().__init__(NodeNotInPathsError.message.format(name=name))