from attrs import Attribute

from typing import Any

class WeetagsException(Exception):
    pass

class NotImplemented(WeetagsException):
    message = """Not implemented"""
    status = 501
    def __init__(self, ) -> None:
        super().__init__(self.message)
        
class ServiceUnavailable(WeetagsException):
    message = """Service Unavailable"""
    status = 503
    def __init__(self, ) -> None:
        super().__init__(self.message)
        
class TreeDoesNotExist(WeetagsException):
    message = """the tree "{name}" is unknown (existing trees: [{trees}])"""
    status = 400
    def __init__(self, name: str, trees: list[str]) -> None:
        trees = ", ".join(trees)
        super().__init__(self.message.format(name=name, trees=trees))

class ParsingError(WeetagsException):
    message = """Parameter(name: {name}, value: {value}) must be of type: {dtype}"""
    status = 400
    def __init__(self, attribute: Attribute, value: Any, dtype_annot: str) -> None:
        super().__init__(self.message.format(name=attribute.name, value=value, dtype=dtype_annot))

class CoversionError(WeetagsException):
    message = """Parameter(value: {value}) Cannot be converted into type: {dtype}"""
    status = 400
    def __init__(self, value: Any, dtype_annot: str) -> None:
        super().__init__(self.message.format(value=value, dtype=dtype_annot))

class UnknownRelation(WeetagsException):
    message = """Relation "{relation}" is unknown. possible relations : [{relations}]"""
    status = 400
    def __init__(self, relation: str, relations: list[str]) -> None:
        relations = ", ".join(relations)
        super().__init__(self.message.format(relation=relation, relations=relations))

class OutputError(WeetagsException):
    message = """{relation} does not return the expected type ({expected})"""
    status = 400
    def __init__(self, relation: str, expected:str) -> None:
        super().__init__(self.message.format(relation=relation, expected=expected))


class MissingLogin(WeetagsException):
    message = """You must provide a username and a password"""
    status = 401
    def __init__(self, ) -> None:
        super().__init__(self.message)

class InvalidLogin(WeetagsException):
    message = """Invalid username or password"""
    status = 401
    def __init__(self, ) -> None:
        super().__init__(self.message)

class InvalidToken(WeetagsException):
    message = """Invalid authorization token"""
    status = 401
    def __init__(self, ) -> None:
        super().__init__(self.message)
        
class AccessDenied(WeetagsException):
    message = """Access denied"""
    status = 401
    def __init__(self, ) -> None:
        super().__init__(self.message)

class AuthorizationTokenRequired(WeetagsException):
    message = """This endpoint is protected. Get an authorization token from `/login` endpoint."""
    status = 401
    def __init__(self, ) -> None:
        super().__init__(self.message)

class OutatedAuthorizationToken(WeetagsException):
    message = """Authorization token is outdated."""
    status = 401
    def __init__(self, ) -> None:
        super().__init__(self.message)

