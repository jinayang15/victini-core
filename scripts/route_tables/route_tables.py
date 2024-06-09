from sqlalchemy import (
    create_engine,
    inspect,
    MetaData,
    Column,
    Integer,
    String,
    Float,
    Table,
)
from sqlalchemy.orm import declarative_base, sessionmaker
import os
from dotenv import load_dotenv

Base = declarative_base()


class OptionalLoops(Base):
    __tablename__ = "optional_loops"

    id = Column(Integer(), primary_key=True, autoincrement=True)
    name = Column(String(50), nullable=False)
    data = Column(Integer(), nullable=False)


def create_route_class(loop_route_name):
    class Route(Base):
        __tablename__ = loop_route_name

        id = Column(Integer(), primary_key=True, autoincrement=True)
        data = Column(Integer(), nullable=False)

        def __init__(self, data):
            self.data = data

    return Route


def check_if_table_exists(table_name, engine):
    return table_name in inspect(engine).get_table_names()


def main(db_user, db_password, db_host, db_name):
    engine = create_engine(
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}/{db_name}"
    )
    try:
        print("Checking for 'optional_loops' table...")
        if not check_if_table_exists("optional_loops", engine):
            print("Table does not exist, terminating.")
            engine.dispose()
            return

        Session = sessionmaker(bind=engine)
        session = Session()

        print("Getting route data...")
        route_names = session.query(OptionalLoops.name).distinct().all()
        # loops through distinct names and creates a new table for each
        for loop in route_names:
            new_route_table = create_route_class(loop.name)
            # drops table if exists
            new_route_table.__table__.drop(engine, checkfirst=True)
            new_route_table.__table__.create(engine)
            session.commit()

            # finds all the data for the current route name
            # equivalent to SELECT name FROM optional_loops WHERE name == route_name
            route_data = (
                session.query(OptionalLoops.data)
                .filter(OptionalLoops.name == loop.name)
                .all()
            )
            # adds all the data belonging to the route
            for d in route_data:
                data = new_route_table(data=d.data)
                session.add(data)
                session.commit()

        session.close()
        print("Script complete")
    finally:
        engine.dispose()


if __name__ == "__main__":
    db_user = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOSTNAME")
    db_name = os.getenv("DB_NAME")
    main(db_user, db_password, db_host, db_name)
