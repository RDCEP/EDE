from sqlalchemy import create_engine
from ede.schema.models import Base

def main():
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/ede', echo=True)
    Base.metadata.create_all(engine)

if __name__ == "__main__":
    main()