from sqlalchemy import create_engine
from ede.schema.models import Base
from ede.config import SQLALCHEMY_DATABASE_URI


def main():
    engine = create_engine(SQLALCHEMY_DATABASE_URI, echo=True)
    Base.metadata.create_all(engine)

if __name__ == "__main__":
    main()