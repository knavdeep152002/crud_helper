from collections.abc import AsyncIterable

from cai_sdk.databases.session import get_organization_database_url
from cai_sdk.dependencies.session import get_organization_name
from cai_sdk.exceptions import CustomException
from fastapi import Depends, status
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base

Base = declarative_base()

db_conn_map: dict[str, Engine] = {}


def close_database_connection_pools():
    global db_conn_map
    for k in db_conn_map:
        db_conn_map[k].dispose()


async def get_db_conn(organization_name=Depends(get_organization_name)) -> Engine:
    global db_conn_map
    if organization_name in db_conn_map:
        return db_conn_map[organization_name]
    db_conn_url = ""
    if __debug__:
        from stageflow.config import conf

        db_conn_url = conf.sqlalchemy_database_url
    else:
        db_conn_url = get_organization_database_url(organization_name)

    if isinstance(db_conn_url, str):
        try:
            db_conn_map[organization_name] = create_engine(db_conn_url)
        except Exception as e:
            raise CustomException(
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Error establishing connection to organization database"
                + (f"\n{e}" if __debug__ else ""),
            )
    else:
        raise CustomException(status=500, detail="invalid db connection url")
    return db_conn_map[organization_name]


# This is the part that replaces sessionmaker
async def get_db_session(
    db_conn: Engine = Depends(get_db_conn),
) -> AsyncIterable[Session]:
    sess = Session(bind=db_conn)
    try:
        yield sess
    finally:
        sess.close()
