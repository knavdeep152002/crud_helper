from typing import TYPE_CHECKING, Any, Generic, Sequence, Tuple, Type, TypeVar

from pydantic import BaseModel, TypeAdapter
from sqlalchemy import ColumnExpressionArgument, select
from sqlalchemy.orm import Session

# FIXME: manage sessions for long running jobs. Handle cases where session may die

JoinData = Tuple[Any, Any]

if TYPE_CHECKING:
    from sqlalchemy.sql._typing import _JoinTargetArgument, _OnClauseArgument

    JoinData = Tuple[_JoinTargetArgument, _OnClauseArgument]


T = TypeVar("T", bound=BaseModel)
U = TypeVar("U", bound=BaseModel)
V = TypeVar("V", bound=Type)


class CRUDHelper(Generic[T, U, V]):
    resource_base: Type[T]
    resource: Type[U]
    resource_db: Type[V]

    def __init__(self, rb: Type[T], r: Type[U], rd: Type[V]) -> None:
        self.resource_base = rb
        self.resource = r
        self.resource_db = rd
        self.resource_type_adapter = TypeAdapter(self.resource)

    def db_row_to_model(self, row: V, validator=None) -> U:
        if validator is not None:
            patch_adapter = TypeAdapter(validator)
            return patch_adapter.validate_python(row.__dict__)
        else:
            return self.resource_type_adapter.validate_python(row.__dict__)

    def db_rows_to_model_list(self, rows: Sequence[V]) -> list[U]:
        return [self.resource_type_adapter.validate_python(r.__dict__) for r in rows]

    def list_resource(
        self,
        session: Session,
        join_data: JoinData | None = None, # type: ignore
        where: list[ColumnExpressionArgument[bool]] | None = None,
    ) -> list[U]:
        stmt = select(self.resource_db)
        if join_data is not None:
            stmt = stmt.join(*join_data)
        if where is not None:
            stmt = stmt.where(*where)
        resources = session.scalars(stmt).all()
        return self.db_rows_to_model_list(resources)

    def get_resource(
        self,
        resource_id: int | None,
        session: Session,
        join_data: JoinData | None = None, # type: ignore
        where: list[ColumnExpressionArgument[bool]] | None = None,
    ) -> U | None:
        if resource_id is None:
            stmt = select(self.resource_db)  # type: ignore
        else:
            stmt = select(self.resource_db).where(self.resource_db.id == resource_id)  # type: ignore
        if join_data is not None:
            stmt = stmt.join(*join_data)
        if where is not None:
            stmt = stmt.where(*where)
        resource = session.scalars(stmt).first()
        if resource is None:
            return None
        return self.db_row_to_model(resource)

    def create_resource(
        self, data: T, session: Session, extra_data: dict[str, Any] | None = None
    ) -> U:
        resource = self.resource_db()  # type: ignore
        data_dict = data.model_dump()
        for k in data_dict:
            setattr(resource, k, data_dict[k])
        if extra_data is not None:
            for k in extra_data:
                setattr(resource, k, extra_data[k])
        session.add(resource)
        session.flush()
        session.refresh(resource)
        return self.db_row_to_model(resource)

    def delete_resource(
        self,
        resource_id: int | None,
        session: Session,
        join_data: JoinData | None = None, # type: ignore
        where: list[ColumnExpressionArgument[bool]] | None = None,
    ) -> U | None:
        if resource_id is None:
            stmt = select(self.resource_db)
        else:
            stmt = select(self.resource_db).where(self.resource_db.id == resource_id)  # type: ignore
        if join_data is not None:
            stmt = stmt.join(*join_data)
        if where is not None:
            stmt = stmt.where(*where)
        resource = session.scalars(stmt).first()
        if resource is None:
            return None
        session.delete(resource)
        session.flush()
        return self.db_row_to_model(resource)

    def update_resource(
        self,
        resource_id: int | None,
        data: T | None,
        session: Session,
        extra_data: dict[str, Any] | None = None,
        join_data: JoinData | None = None, # type: ignore
        where: list[ColumnExpressionArgument[bool]] | None = None,
        validator=None,
    ) -> U | None:
        if resource_id is None:
            stmt = select(self.resource_db)  # type: ignore
        else:
            stmt = select(self.resource_db).where(self.resource_db.id == resource_id)  # type: ignore
        if join_data is not None:
            stmt = stmt.join(*join_data)
        if where is not None:
            stmt = stmt.where(*where)
        resource = session.scalars(stmt).first()
        if resource is None:
            return None
        if data is not None:
            data_dict = data.model_dump(exclude_unset=True)
            for k in data_dict:
                setattr(resource, k, data_dict[k])

        if extra_data is not None:
            for k in extra_data:
                setattr(resource, k, extra_data[k])
        session.add(resource)
        session.flush()
        session.refresh(resource)
        return self.db_row_to_model(resource, validator=validator)
