from sqlalchemy import create_engine
from ede.schema.models import Base
from ede.credentials import DB_NAME, DB_PASS, DB_PORT, DB_USER, DB_HOST

def main():
    engine = create_engine('postgresql://%s:%s@%s:%s/%s' % (DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME))
    Base.metadata.create_all(engine)

if __name__ == "__main__":
    main()
