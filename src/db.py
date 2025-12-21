from sqlalchemy import create_engine


def get_engine():
    return create_engine(
        "postgresql+psycopg://dbd-25:dbd-25@localhost:5432/dbd-25", echo=False
    )
